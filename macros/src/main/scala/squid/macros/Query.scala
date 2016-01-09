package squid.macros

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.internal.util.OffsetPosition
import scala.reflect.macros.whitebox

import com.typesafe.config.ConfigFactory

import squid.meta.RegisterType
import squid.protocol._

class Query extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro Query.impl
}

object Query {
  def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.universe.Tree = {
    import c.universe._

    val mod = c.mirror.staticModule(config.typeRegistryName)
    val typ = mod.moduleClass.asClass.toType

    final case class RetVal(name: String, typeName: PGTypeName, nullable: Boolean)

    final case class TypeRegistryInfo(method: TermName, returnType: Type)

    def lookupTypeRegistryInfo(typeName: PGTypeName): TypeRegistryInfo = {
      typ.baseClasses.foreach { bc =>
        bc.asClass.toType.decls.foreach { d =>
          d.annotations.foreach { ann =>
            if (ann.tree.tpe =:= typeOf[RegisterType]) {
              ann.tree.children.tail match {
                case List(Literal(Constant(ns: String)), Literal(Constant(tn: String))) =>
                  if (typeName == PGTypeName(ns, tn)) {
                    return TypeRegistryInfo(
                      d.name.toTermName,
                      d.typeSignature.resultType
                    )
                  }
                case _ =>
                  c.abort(c.enclosingPosition, s"Invalid annotation arguments in $d of $bc")
              }
            }
          }
        }
      }
      c.abort(c.enclosingPosition, s"Type '$typeName' not registered in $mod")
    }

    def getRetValTypeTree(rv: RetVal): Tree = {
      val info = lookupTypeRegistryInfo(rv.typeName)
      val typeTree = q"${info.returnType}"
      if (rv.nullable) {
        AppliedTypeTree(
          Ident(TypeName("Option")),
          List(typeTree)
        )
      } else {
        typeTree
      }
    }

    /** Returns the SQL string parts and the list of parameter names */
    def findSQL(template: Template): (List[String], List[TermName]) = {
      template.body.view.flatMap {
        case Literal(Constant(s: String)) => Some((List(s), Nil))
        case t => extractSQLFromStringContext(t)
      }.headOption.getOrElse {
        c.abort(c.enclosingPosition, "Missing SQL query")
      }
    }

    /**
      * If the provided Tree is a StringContext, return the SQL and parameter names.
      * Note that the SQL will now have $1 parameters in place of the interpolated values.
      */
    def extractSQLFromStringContext(t: Tree): Option[(List[String], List[TermName])] = {
      Some(t).collect {
        case Apply(Select(Apply(Ident(TermName("StringContext")), parts), TermName("s")), params) =>
          val sqlStrings = parts.map {
            case Literal(Constant(s: String)) => s
            case other => c.abort(c.enclosingPosition, s"Invalid StringContext part: $other")
          }

          val termNames = params.map {
            case Ident(termName: TermName) => termName
            case other => c.abort(c.enclosingPosition, s"Unsupported parameter: $other")
          }
          (sqlStrings, termNames)
      }
    }

    def buildSQLForProtocol(parts: List[String]): String = {
      val tokens = Stream.from(1).map("$" + _)
      // Interleave the parts and the tokens.
      // Use .init to ignore the last token since we just need tokens between parts.
      parts.zip(tokens).flatMap(x => List(x._1, x._2)).init.mkString
    }

    def buildSQLForJDBC(parts: List[String]): String = parts.mkString("?")

    // Attempt to find the actual source position of the error inside the SQL statement.
    def handleError(err: PGResponse.Error, sql: String): Nothing = err.response match {
      case Some(r) if r.getPosition.isDefined =>
        val pos = r.getPosition.get
        val sourceChars = c.enclosingPosition.source.content
        var quoteCount = 0
        var i = c.enclosingPosition.start
        while (i < c.enclosingPosition.source.length && quoteCount < 3) {
          if (sourceChars(i) == '"') quoteCount += 1
          i += 1
        }
        val sqlPos = i + pos - 1
        val newPos = new OffsetPosition(c.enclosingPosition.source, sqlPos)
        c.abort(newPos.asInstanceOf[c.Position], "\nPostgreSQL: " + r.getMessage)

      case _ => c.abort(c.enclosingPosition, err.message)
    }

    def getRetVals(conn: PGConnection, sql: String): List[RetVal] = {
      // TODO: Get types of passed params
      conn.query.describe(sql, types = Nil).successOr(err =>
        handleError(err, sql)
      ).columns.map { col =>
        val typeName = conn.types.getName(col.colType).getOrElse {
          c.abort(c.enclosingPosition,
            s"Type not found for oid ${col.colType} for column $col"
          )
        }
        RetVal(
          col.name,
          typeName,
          col.nullable
        )
      }
    }

    def getRowCtorArgs(retVals: List[RetVal]): List[ValDef] = {
      retVals.map(rv =>
        ValDef(
          Modifiers(Flag.CASEACCESSOR | Flag.PARAMACCESSOR),
          TermName(rv.name),
          getRetValTypeTree(rv),
          EmptyTree
        )
      )
    }

    // Param for: def rsToRow(rs: ResultSet): Row
    val rsParam = TermName("rs")

    def buildResultSetColumnGetter(rv: RetVal, index: Int): Tree = {
      val info = lookupTypeRegistryInfo(rv.typeName)
      val baseGetter = q"$mod.${info.method}($rsParam, $index)"
      if (rv.nullable) q"Option($baseGetter)" else baseGetter
    }

    def getRsToRowArgs(retVals: List[RetVal]): List[Tree] = {
      retVals.zipWithIndex.map { case (rv, i) =>
        buildResultSetColumnGetter(rv, i + 1)
      }
    }

    annottees match {
      // @Query object Foo { .. }
      case List(Expr(ModuleDef(_, name, template))) =>
        val (sqlParts, params) = findSQL(template)
        val protocolSQL = buildSQLForProtocol(sqlParts)
        val jdbcSQL = buildSQLForJDBC(sqlParts)
        if (params.nonEmpty) {
          c.abort(c.enclosingPosition, s"Params not defined: $params")
        }
        val retVals = withProtocol { conn => getRetVals(conn, protocolSQL) }
        val rowCtorArgs = getRowCtorArgs(retVals)
        val rsToRowArgs = getRsToRowArgs(retVals)

        q"""
          object $name {
            case class Row(..$rowCtorArgs)
            type Result = Stream[Row]
            val sql = $jdbcSQL

            def rsToRow($rsParam: java.sql.ResultSet): Row = {
              Row(..$rsToRowArgs)
            }

            def fetch()(implicit c: java.sql.Connection): Result = {
              squid.meta.DBUtils.executeQueryStream(sql, rsToRow)
            }
          }
        """

      // @Query class Foo(...) { ... }
      case List(Expr(ClassDef(mods, name, List(), template))) =>
        // First ValDefs are the class constructor args.
        val valDefs = template.body.view.map {
          case v: ValDef => Some(v)
          case _ => None
        }.takeWhile(_.isDefined).map(_.get).toList

        val applyParams = valDefs.map {
          case ValDef(_, valName, valType, _) =>
            ValDef(
              Modifiers(Flag.PARAM),
              valName,
              valType,
              EmptyTree
            )
        }

        val objName = name.toTermName
        val classCtorArgs = applyParams.map(_.name)
        val (sqlParts, params) = findSQL(template)
        val protocolSQL = buildSQLForProtocol(sqlParts)
        val jdbcSQL = buildSQLForJDBC(sqlParts)
        val retVals = withProtocol { conn => getRetVals(conn, protocolSQL) }
        val rowCtorArgs = getRowCtorArgs(retVals)
        val rsToRowArgs = getRsToRowArgs(retVals)

        val setStatementsCode = params.zipWithIndex.map { case (p, i) =>
          q""" squid.meta.StatementParam.set(st, ${i + 1}, $p) """
        }

        q"""
          object $objName {
            case class Row(..$rowCtorArgs)
            type Result = Stream[Row]
            val sql = $jdbcSQL

            def rsToRow($rsParam: java.sql.ResultSet): Row = {
              Row(..$rsToRowArgs)
            }

            def apply(..$applyParams): $name = new $name(..$classCtorArgs)
          }

          class $name(..$valDefs) {
            def fetch()(implicit c: java.sql.Connection): $objName.Result = {
              val st = c.prepareStatement($objName.sql)
              $setStatementsCode
              val rs = st.executeQuery()
              squid.meta.DBUtils.streamResultSet(st, rs).map($objName.rsToRow)
            }
          }
        """

      case _ =>
        c.abort(c.enclosingPosition, s"Invalid @Query class: ${showRaw(annottees)}")
    }
  }

  private def withProtocol[A](block: PGConnection => A): A = {
    PGProtocol.withConnection(config.protocolInfo)(block)
  }

  private object config {
    // TODO: ConfigFactory.load() doesn't seem to work, for some reason need to supply
    // the class loader.  Need to ensure this works for user-supplied configs.
    lazy val c = ConfigFactory.load(getClass.getClassLoader)
    lazy val protocolInfo = PGConnectInfo.fromConfig(c)
    lazy val typeRegistryName = c.getString("squid.typeRegistry")
  }
}
