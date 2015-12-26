package squid.protocol

import org.specs2.Specification

class PGProtocolSpec extends Specification { def is = s2"""
  PGProtocol should
    start up $startUp
    describe $describe
  """

  def startUp = PGProtocol.withConnection(INFO) { c =>
    c.getState === PGState.Idle
  }

  def describe = PGProtocol.withConnection(INFO) { implicit c =>
    val result = PGProtocol.describe("select * from foo.bar", Nil)
    result.paramTypes === Nil and
      result.columns.map(_.name) === List("id", "quux") and
      result.columns.map(_.nullable) === List(true, true)
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
