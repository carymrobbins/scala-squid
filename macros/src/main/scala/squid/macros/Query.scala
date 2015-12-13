package squid.macros

import java.sql.DriverManager

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

import com.typesafe.config.ConfigFactory

import squid.meta.{RegisterType, TableMetaData, Typer}
import squid.parser.PGParser

class Query extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro Query.impl
}

object Query {
  def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.universe.Tree = {
    import c.universe._

    val mod = c.mirror.staticModule(config.typeRegistryName)
    val typ = mod.moduleClass.asClass.toType

    case class TypeRegistryInfo(method: TermName, returnType: Type)

    // Turn this into a Map
    def lookupTypeRegistryInfo(typeName: String): TypeRegistryInfo = {
      typ.baseClasses.foreach { bc =>
        bc.asClass.toType.decls.foreach { d =>
          d.annotations.foreach { ann =>
            if (ann.tree.tpe =:= typeOf[RegisterType]) {
              ann.tree.children.tail match {
                case List(Literal(Constant(registeredTypeName: String))) =>
                  if (registeredTypeName == typeName) {
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

    def getRetValTypeTree(rv: Typer.RetVal): Tree = {
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

    /** Returns the SQL string and the list of parameter names */
    def findSQL(template: Template): (String, List[TermName]) = {
      template.body.view.flatMap {
        case Literal(Constant(s: String)) => Some((s, Nil))
        case t => extractSQLFromStringContext(t)
      }.headOption.getOrElse {
        c.abort(c.enclosingPosition, "Missing SQL query")
      }
    }

    /**
      * If the provided Tree is a StringContext, return the SQL and parameter names.
      * Note that the SQL will now have $1 parameters in place of the interpolated values.
      */
    def extractSQLFromStringContext(t: Tree): Option[(String, List[TermName])] = {
      Some(t).collect {
        case Apply(Select(Apply(Ident(TermName("StringContext")), parts), TermName("s")), params) =>
          // Inject the parameter tokens.
          val sql = parts.map {
            case Literal(Constant(part: String)) => part
            case other => c.abort(c.enclosingPosition, s"Invalid StringContext part: $other")
          }.mkString("?")

          val termNames = params.map {
            case Ident(termName: TermName) => termName
            case other => c.abort(c.enclosingPosition, s"Unsupported parameter: $other")
          }
          (sql, termNames)
      }
    }

    def getRetVals(sql: String): List[Typer.RetVal] = {
      val ast = PGParser.parse(sql).fold(
        err => c.abort(c.enclosingPosition, s"Error parsing SQL: $err"),
        identity
      )
      Typer.getReturnValues(ast)(ConnectionFactory.tableMetaData)
    }

    def getRowCtorArgs(retVals: List[Typer.RetVal]): List[ValDef] = {
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

    def buildResultSetColumnGetter(rv: Typer.RetVal, index: Int): Tree = {
      val info = lookupTypeRegistryInfo(rv.typeName)
      val baseGetter = q"$mod.${info.method}($rsParam, $index)"
      if (rv.nullable) q"Option($baseGetter)" else baseGetter
    }

    def getRsToRowArgs(retVals: List[Typer.RetVal]): List[Tree] = {
      retVals.zipWithIndex.map { case (rv, i) =>
        buildResultSetColumnGetter(rv, i + 1)
      }
    }

    annottees match {
      // @Query object Foo { .. }
      case List(Expr(ModuleDef(_, name, template))) =>
        val (sql, params) = findSQL(template)
        if (params.nonEmpty) {
          c.abort(c.enclosingPosition, s"Params not defined: $params")
        }
        val retVals = getRetVals(sql)
        val rowCtorArgs = getRowCtorArgs(retVals)
        val rsToRowArgs = getRsToRowArgs(retVals)

        q"""
          object $name {
            case class Row(..$rowCtorArgs)
            type Result = Stream[Row]
            val sql = $sql

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
        val (sql, params) = findSQL(template)
        val retVals = getRetVals(sql)
        val rowCtorArgs = getRowCtorArgs(retVals)
        val rsToRowArgs = getRsToRowArgs(retVals)

        val setStatementsCode = params.zipWithIndex.map { case (p, i) =>
          q""" squid.meta.StatementParam.set(st, ${i + 1}, $p) """
        }

        q"""
          object $objName {
            case class Row(..$rowCtorArgs)
            type Result = Stream[Row]
            val sql = $sql

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

  private object ConnectionFactory {
    lazy val connection = {
      Class.forName(config.driver)
      DriverManager.getConnection(config.jdbcUrl, config.username, config.password)
      // TODO: Close the Connection
    }

    lazy val tableMetaData = {
      TableMetaData.init()(connection)
    }
  }

  private object config {
    // TODO: ConfigFactory.load() doesn't seem to work, for some reason need to supply
    // the class loader.  Need to ensure this works for user-supplied configs.
    lazy val c = ConfigFactory.load(getClass.getClassLoader)
    lazy val driver = c.getString("squid.driverClassName")
    lazy val jdbcUrl = c.getString("squid.jdbcUrl")
    lazy val username = c.getString("squid.username")
    lazy val password = c.getString("squid.password")
    lazy val typeRegistryName = c.getString("squid.typeRegistry")
  }
}
