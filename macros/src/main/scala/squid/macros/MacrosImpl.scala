package squid.macros

import java.io.File
import java.sql.DriverManager

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

import com.typesafe.config.ConfigFactory
import squid.parser.PGParser
import squid.meta.{TableMetaData, Typer}

class Query extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro Query.impl
}

trait ConnectionInfo {
  val jdbcDriver: String
  val jdbcUrl: String
  val jdbcUser: String
  val jdbcPassword: String
}

object Query {
  def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.universe.Tree = {
    import c.universe._

    def getRetValTypeTree(rv: Typer.RetVal): Tree = {
      if (rv.nullable) {
        AppliedTypeTree(
          Ident(TypeName("Option")),
          List(Ident(TypeName(rv.typ)))
        )
      } else {
        Ident(TypeName(rv.typ))
      }
    }

    // TODO: This sucks, we need some sort of typeclass describing the logic
    // instead of building the method call from strings.
    def buildResultSetColumnGetter(rv: Typer.RetVal, rsParam: TermName, index: Int): Tree = {
      val method = rv.typ match {
        case "String" => "getString"
        case "Long" => "getLong"
        case other => throw new RuntimeException(s"Return value '$other' not supported")
      }

      val baseGetter = q"$rsParam.${TermName(method)}($index)"

      if (rv.nullable) q"Option($baseGetter)" else baseGetter
    }

    annottees match {
      case List(Expr(ClassDef(mods, name, List(), template))) =>
        if (mods != Modifiers(Flag.CASE)) {
          c.abort(c.enclosingPosition, "Only case classes are supported")
        }
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
              case class $name() {
                case class Row(..$rowCtorArgs)

                type Result = Stream[Row]

                val sql = $sql

                def rsToRow($rs: java.sql.ResultSet): Row = {
                  Row(..$rsToRowArgs)
                }

                def execute()(implicit c: Connection): Result = {
                  squid.meta.DBUtils.executeQueryStream(sql, rsToRow)
                }
              }
            """

          case _ =>
            c.abort(c.enclosingPosition, "Invalid @Query class body")
        }

      case _ =>
        c.abort(c.enclosingPosition, "Invalid @Query class")
    }
  }

  private object ConnectionFactory {
    lazy val connection = {
      // TODO: Don't hard code the config file path, but `.load()` doesn't seem to work.
      val config = ConfigFactory.parseFile(
        new File("macros/src/test/resources/squid.conf")
      ).resolve()
      Class.forName(config.getString("driverClassName"))
      DriverManager.getConnection(
        config.getString("jdbcUrl"),
        config.getString("username"),
        config.getString("password")
      )
      // TODO: Close the Connection
    }

    lazy val tableMetaData = {
      TableMetaData.init()(connection)
    }
  }
}

