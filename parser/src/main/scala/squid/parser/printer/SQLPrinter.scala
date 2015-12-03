package squid.parser.printer

import squid.parser.ast._

/**
  * Pretty-print PostgreSQL ASTs
  */
object SQLPrinter {
  // def print(sql: SQL): String
}

trait Pretty[A] {
  def pretty(a: A): String
}

object Pretty {
  def apply[A](f: A => String): Pretty[A] = new Pretty[A] {
    override def pretty(a: A): String = f(a)
  }
}
