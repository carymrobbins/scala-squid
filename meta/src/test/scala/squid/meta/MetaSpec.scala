package squid.meta

import java.sql._

import squid.parser.PGParser
import squid.test.DBSpecBase

/**
  * TODO
  */
class MetaSpec extends DBSpecBase { def is = s2"""
  Meta should
    retrieve basic column info $basicColumnInfo
    fetch table meta data $fetchTableMetaData
    basic select type info $basicSelectTypeInfo
  """

  def basicColumnInfo = withRollback { implicit c =>
    val cols = Meta.getColumns(None, Some("foo"), "bar")
    cols === List(
      Meta.Column("id", Types.INTEGER, "int4", nullable = false),
      Meta.Column("quux", Types.VARCHAR, "text", nullable = true)
    )
  }

  def fetchTableMetaData = withRollback { implicit c =>
    withTableMetaData { tmd =>
      tmd.get(Some("foo"), "bar") === Some(
        List(
          Meta.Column("id", Types.INTEGER, "int4", nullable = false),
          Meta.Column("quux", Types.VARCHAR, "text", nullable = true)
        )
      )
    }
  }

  def basicSelectTypeInfo = withRollback { implicit c =>
    withTableMetaData { implicit tmd =>
      val ast = PGParser.unsafeParse("select id, quux from foo.bar")
      val returnValues = Typer.getReturnValues(ast)
      returnValues === List(
        Typer.RetVal("id", "int4", nullable = false),
        Typer.RetVal("quux", "text", nullable = true)
      )
    }
  }
}
