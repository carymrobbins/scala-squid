package squid.protocol

import org.specs2.Specification

class PGProtocolSpec extends Specification { def is = s2"""
  PGProtocol should
    startUp $startUp
    describe $describe
    preparedQuery $preparedQuery
  """

  def startUp = PGProtocol.withConnection(INFO) { c =>
    c.getState === PGState.Idle
  }

  def describe = PGProtocol.withConnection(INFO) { c =>
    val result = c.describe("select * from foo.bar", Nil)
    result.paramTypes === Nil and
      result.columns === List(
        DescribeColumn("id", c.getTypeOID[Int](), nullable = false),
        DescribeColumn("quux", c.getTypeOID[String](), nullable = true)
      )
  }

  def preparedQuery = PGProtocol.withConnection(INFO) { c =>
    c.preparedQuery("select * from foo.bar", Nil, Nil).toList === List(
      List(PGValue.Text("1"), PGValue.Text("alpha")),
      List(PGValue.Text("2"), PGValue.Text("bravo")),
      List(PGValue.Text("3"), PGValue.Text("charlie"))
    )
  }

  private val INFO = PGConnectInfo(
    host = "localhost",
    port = 5432,
    timeout = 5,
    user = "squid",
    password = "squid",
    database = "squid",
    debug = false
  )
}
