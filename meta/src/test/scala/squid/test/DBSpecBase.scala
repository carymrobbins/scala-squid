package squid.test

import java.sql.{DriverManager, Connection}

import com.typesafe.config.ConfigFactory
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import squid.meta.TableMetaData

/**
  * Created by crobbins on 12/12/15.
  */
abstract class DBSpecBase extends Specification {
  /** Helper to easily combine a sequence of MatchResults. */
  def sequence[T](ms: MatchResult[T]*): MatchResult[Seq[T]] = MatchResult.sequence(ms)

  /** Performs block in a transaction and rolls back to avoid side effects. */
  def withRollback[A](block: Connection => A): A = {
    val c = getConnection()
    c.setAutoCommit(false)
    try {
      block(c)
    } finally {
      c.rollback()
      c.close()
    }
  }

  def withTableMetaData[A](block: TableMetaData => A)(implicit c: Connection): A = {
    block(TableMetaData.init())
  }

  //noinspection AccessorLikeMethodIsEmptyParen
  def getConnection(): Connection = {
    Class.forName(config.getString("squid.driverClassName"))
    DriverManager.getConnection(
      config.getString("squid.jdbcUrl"),
      config.getString("squid.username"),
      config.getString("squid.password")
    )
  }

  lazy val config = ConfigFactory.load()
}
