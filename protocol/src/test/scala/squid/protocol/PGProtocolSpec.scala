package squid.protocol

import com.typesafe.config.ConfigFactory
import org.specs2.Specification

class PGProtocolSpec extends Specification { def is = s2"""
  PGProtocol should
    startUp $startUp
    describe $describe
    preparedQuery $preparedQuery
    getTypeName $getTypeName
    syntaxError $syntaxError
  """

  def startUp = withConnection { c =>
    c.state === PGState.Idle
  }

  def describe = withConnection { c =>
    val result = c.query.describe("select * from foo.bar", Nil).getOrThrow
    result.paramTypes === Nil and
      result.columns === List(
        DescribeColumn("id", c.types.getOID[Int](), nullable = false),
        DescribeColumn("quux", c.types.getOID[String](), nullable = true)
      )
  }

  def preparedQuery = withConnection { c =>
    c.query.prepared("select * from foo.bar", Nil, Nil).toList === List(
      List(PGValue.Text("1"), PGValue.Text("alpha")),
      List(PGValue.Text("2"), PGValue.Null),
      List(PGValue.Text("3"), PGValue.Text("charlie"))
    )
  }

  def getTypeName = withConnection { c =>
    val cols = c.query.describe("select * from foo.bar", Nil).getOrThrow.columns
    val typeNames = cols.map(col => c.types.getName(col.colType))
    typeNames === List(
      Some(PGTypeName("pg_catalog", "int4")),
      Some(PGTypeName("pg_catalog", "text"))
    )
  }

  def syntaxError = withConnection { c =>
    c.query.describe("select 1 from 2", Nil) must beAnInstanceOf[PGResponse.Error]
  }

  private def withConnection[A](block: PGConnection => A): A = {
    PGProtocol.withConnection(INFO)(block)
  }

  private val config = ConfigFactory.load()

  private val INFO = PGConnectInfo.fromConfig(config).copy(
    debug = false
  )
}
