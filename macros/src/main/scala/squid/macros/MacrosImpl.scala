package squid.macros

import java.sql.{ResultSet, DriverManager}

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

import com.typesafe.config.ConfigFactory

import squid.meta.{TableMetaData, Typer}
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

    // TODO: This sucks, we need some sort of typeclass describing the logic
    // instead of building the method call from strings.
    def buildResultSetColumnGetter(rv: Typer.RetVal, rsParam: TermName, index: Int): Tree = {
      val info = lookupTypeRegistryInfo(rv.typeName)
      val baseGetter = q"$mod.${info.method}($rsParam, $index)"
      if (rv.nullable) q"Option($baseGetter)" else baseGetter
    }

    annottees match {
      // @Query object Foo { .. }
      case List(Expr(ModuleDef(_, name, template))) =>
        template.body match {
          case List(_: DefDef, Literal(Constant(sql: String))) =>
            val ast = PGParser.parse(sql).fold(
              err => c.abort(c.enclosingPosition, s"Error parsing SQL: $err"),
              identity
            )
            val retVals = Typer.getReturnValues(ast)(ConnectionFactory.tableMetaData)

            val rowCtorArgs = retVals.map(rv =>
              ValDef(
                Modifiers(Flag.CASEACCESSOR | Flag.PARAMACCESSOR),
                TermName(rv.name),
                getRetValTypeTree(rv),
                EmptyTree
              )
            )

            val rs = TermName("rs")

            val rsToRowArgs = retVals.zipWithIndex.map { case (rv, i) =>
              buildResultSetColumnGetter(rv, rs, i + 1)
            }

            q"""
              object $name {
                case class Row(..$rowCtorArgs)

                type Result = Stream[Row]

                val sql = $sql

                def rsToRow($rs: java.sql.ResultSet): Row = {
                  Row(..$rsToRowArgs)
                }

                def fetch()(implicit c: java.sql.Connection): Result = {
                  squid.meta.DBUtils.executeQueryStream(sql, rsToRow)
                }
              }
            """

          // TODO @Query class Foo() { ... }
          //case List(Expr(ClassDef(mods, name, List(), template))) =>

          case _ =>
            c.abort(c.enclosingPosition, "Invalid @Query class body")
        }

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

case class RegisterType(typeName: String) extends StaticAnnotation

trait BaseTypeRegistry {
  @RegisterType("int4")
  def toInt4(rs: ResultSet, index: Int): Long = rs.getLong(index)

  @RegisterType("text")
  def toText(rs: ResultSet, index: Int): String = rs.getString(index)
}

object TypeRegistry extends BaseTypeRegistry
