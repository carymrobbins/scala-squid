package squid.macros

import java.sql.{Connection, DriverManager}

import com.typesafe.config.ConfigFactory
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
    DBUtils.execute("insert into foo.bar values (1, 'a'), (2, null)")
    Bars.fetch().toList === List(
      Bars.Row(1, Some("a")),
      Bars.Row(2, None)
    )
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
  private def getConnection(): Connection = connection

  private val config = ConfigFactory.load()
  private lazy val connection = {
    Class.forName(config.getString("squid.driverClassName"))
    DriverManager.getConnection(
      config.getString("squid.jdbcUrl"),
      config.getString("squid.username"),
      config.getString("squid.password")
    )
  }
}

@Query object Bars {"""
  select id, quux
  from foo.bar
  order by id
"""}
