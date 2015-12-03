package squid.meta

import java.sql.{Connection, ResultSet}

/**
  * Database utils to simplify using JDBC.
  * Should probably use something like ScalikeJDBC.
  */
object DBUtils {
  import DBImplicits._

  def execute(sql: String)(implicit c: Connection): Unit = {
    val st = c.createStatement()
    st.execute(sql)
    st.close()
  }

  def executeQuery(sql: String)(implicit c: Connection): ResultSet = {
    val st = c.createStatement()
    st.executeQuery(sql)
    // TODO: Close the statement
  }

  // NOTE: This is used by the macros
  def executeQueryStream[A]
      (sql: String, f: ResultSet => A)(implicit c: Connection): Stream[A] = {
    executeQuery(sql).map(f)
  }
}

object DBImplicits {
  final implicit class RichResultSet(val underlying: ResultSet) {
    def map[A](f: ResultSet => A): Stream[A] = {
      Stream.continually(underlying).takeWhile(_.next()).map(f)
      // TODO: Close the ResultSet
    }
  }
}
