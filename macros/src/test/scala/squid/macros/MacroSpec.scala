package squid.macros

import java.sql.{Connection, DriverManager}

import org.specs2.Specification

import squid.meta.DBUtils

/**
  * TODO
  */
class MacroSpec extends Specification { def is = s2"""
    MacroSpec
      simple query list $simpleQueryList
  """

  def simpleQueryList = withRollback { implicit c =>
    DBUtils.execute("insert into foo.bar values (1, 'a'), (2, 'b')")
    Queries.GetBars().execute().map(row =>
      (row.id, row.quux)
    ).toList === List(
      (1, Some("a")),
      (2, Some("b"))
    )
  }

  implicit final class RichString(val underlying: String) {
    def matchesWithoutSpaces(s: String) = {
      whiteSpaceRegex.split(underlying) === whiteSpaceRegex.split(s)
    }
  }

  private val whiteSpaceRegex = "\\s+".r

  // COPY PASTA FROM MetaSpec (sort of)

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
    // Ensure driver is loaded.
    driver
    DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
  }

  private lazy val driver = Class.forName("org.postgresql.Driver")
  private val jdbcUrl = "jdbc:postgresql://localhost:5432/squid"
  private val jdbcUser = "squid"
  private val jdbcPassword = "squid"
}

object Queries {

  @Query
  case class GetBars() {"""
    select id, quux
    from foo.bar
    order by id
  """}
}
