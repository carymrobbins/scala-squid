package squid.meta

import java.sql.{Types, PreparedStatement}

/**
  * Typeclass describing how to set a prepared statement parameter.
  */
trait StatementParam[A] {
  def set(statement: PreparedStatement, index: Int, value: A): Unit
}

object StatementParam {
  def apply[A](f: (PreparedStatement, Int, A) => Unit): StatementParam[A] = new StatementParam[A] {
    override def set(statement: PreparedStatement, index: Int, value: A): Unit = {
      f(statement, index, value)
    }
  }

  def set[A: StatementParam](st: PreparedStatement, i: Int, v: A): Unit = {
    implicitly[StatementParam[A]].set(st, i, v)
  }

  // Instances

  implicit val spInt: StatementParam[Int] = StatementParam(
    (st, i, v) => st.setInt(i, v)
  )

  implicit val spLong: StatementParam[Long] = StatementParam(
    (st, i, v) => st.setLong(i, v)
  )

  implicit val spString: StatementParam[String] = StatementParam(
    (st, i, v) => st.setString(i, v)
  )

  implicit def spOption[A : StatementParam]: StatementParam[Option[A]] = StatementParam(
    (st, i, o) => o match {
      case None =>
        // TODO: Create a typeclass which returns the appropriate type so we
        // won't use the NULL type here.
        st.setNull(i, Types.NULL)

      case Some(v) =>
        StatementParam.set(st, i, v)
    }
  )
}
