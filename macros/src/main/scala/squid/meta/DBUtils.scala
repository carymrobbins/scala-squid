package squid.meta

import java.sql.{Statement, Connection, ResultSet}

/**
  * Database utils to simplify using JDBC.
  * Should probably use something like ScalikeJDBC.
  */
object DBUtils {

  def execute(sql: String)(implicit c: Connection): Unit = {
    val st = c.createStatement()
    st.execute(sql)
    st.close()
  }

  // NOTE: This is used by the macros
  def executeQueryStream[A]
      (sql: String, f: ResultSet => A)(implicit c: Connection): Stream[A] = {
    val st = c.createStatement()
    streamResultSet(st, st.executeQuery(sql)).map(f)
  }

  def streamResultSet(st: Statement, rs: ResultSet): Stream[ResultSet] = {
    if (!rs.isBeforeFirst || rs.getMetaData.getColumnCount == 0) {
      rs.close()
      st.close()
      Stream.empty
    } else {
      Stream.continually(rs).takeWhile { rs =>
        val hasNext = rs.next()
        if (!hasNext) {
          // TODO: What if we never traverse the whole ResultSet?
          rs.close()
          st.close()
        }
        hasNext
      }
    }
  }

  def streamResultSet(rs: ResultSet): Stream[ResultSet] = {
    if (!rs.isBeforeFirst) {
      rs.close()
      Stream.empty
    } else {
      Stream.continually(rs).takeWhile { rs =>
        val hasNext = rs.next()
        if (!hasNext) {
          // TODO: What if we never traverse the whole ResultSet?
          rs.close()
        }
        hasNext
      }
    }
  }
}
