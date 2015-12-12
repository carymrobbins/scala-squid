package squid.meta

import java.sql._

import scala.collection.mutable

import com.typesafe.config.ConfigFactory
import org.specs2._
import squid.parser.PGParser

/**
  * TODO
  */
class MetaSpec extends Specification { def is = s2"""
  Meta should
    retrieve basic column info $basicColumnInfo
    fetch table meta data $fetchTableMetaData
    basic select type info $basicSelectTypeInfo
  """

  def basicColumnInfo = withRollback { implicit c =>
    val cols = Meta.getColumns(None, Some("foo"), "bar")
    cols === List(
      Meta.Column("baz", Types.INTEGER, "int4", nullable = false),
      Meta.Column("quux", Types.VARCHAR, "text", nullable = true)
    )
  }

  def fetchTableMetaData = withRollback { implicit c =>
    withTableMetaData { tmd =>
      tmd.underlying === mutable.Map(
        TableMetaData.Key(Some("foo"), "bar") -> List(
          Meta.Column("baz", Types.INTEGER, "int4", nullable = false),
          Meta.Column("quux", Types.VARCHAR, "text", nullable = true)
        )
      )
    }
  }

  def basicSelectTypeInfo = withRollback { implicit c =>
    withTableMetaData { implicit tmd =>
      val ast = PGParser.unsafeParse("select baz, quux from foo.bar")
      val returnValues = Typer.getReturnValues(ast)
      returnValues === List(
        Typer.RetVal("baz", "int4", nullable = false),
        Typer.RetVal("quux", "text", nullable = true)
      )
    }
  }

  private def withTableMetaData[A](block: TableMetaData => A)(implicit c: Connection): A = {
    block(TableMetaData.init())
  }

  /** Performs block in a transaction and rolls back to avoid side effects. */
  private def withRollback[A](block: Connection => A): A = {
    val c = getConnection()
    c.setAutoCommit(false)
    try {
      block(c)
    } finally {
      c.rollback()
      c.close()
    }
  }

  //noinspection AccessorLikeMethodIsEmptyParen
  private def getConnection(): Connection = {
    connection match {
      case Some(c) => c
      case None =>
        val config = ConfigFactory.load()
        Class.forName(config.getString("driverClassName"))
        val c = DriverManager.getConnection(
          config.getString("jdbcUrl"),
          config.getString("username"),
          config.getString("password")
        )
        connection = Some(c)
        c
    }
  }

  private var connection: Option[Connection] = None
}
