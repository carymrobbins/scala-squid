package squid.protocol

import com.typesafe.config.ConfigFactory
import org.specs2.Specification

class PGProtocolSpec extends Specification { def is = s2"""
  PGProtocol should
    startUp $startUp
    describe $describe
    preparedQuery $preparedQuery
    getTypeName $getTypeName
  """

  def startUp = withConnection { c =>
    c.getState === PGState.Idle
  }

  def describe = withConnection { c =>
    val result = c.describe("select * from foo.bar", Nil)
    result.paramTypes === Nil and
      result.columns === List(
        DescribeColumn("id", c.getTypeOID[Int](), nullable = false),
        DescribeColumn("quux", c.getTypeOID[String](), nullable = true)
      )
  }

  def preparedQuery = withConnection { c =>
    c.preparedQuery("select * from foo.bar", Nil, Nil).toList === List(
      List(PGValue.Text("1"), PGValue.Text("alpha")),
      List(PGValue.Text("2"), PGValue.Null),
      List(PGValue.Text("3"), PGValue.Text("charlie"))
    )
  }

  def getTypeName = withConnection { c =>
    val cols = c.describe("select * from foo.bar", Nil).columns
    val typeNames = cols.map(col => c.getTypeName(col.colType))
    typeNames === List(
      PGTypeName("pg_catalog", "int4"),
      PGTypeName("pg_catalog", "text")
    )
  }

  private def withConnection[A](block: PGConnection => A): A = {
    PGProtocol.withConnection(INFO)(block)
  }

  private val config = ConfigFactory.load()

  private val INFO = PGConnectInfo.fromConfig(config).copy(
    debug = false
  )
}
