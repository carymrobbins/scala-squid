package squid.parser

import com.typesafe.config.ConfigFactory
import squid.parser.ast.SQL

/** Consumes output from queryparser */
object PGParser {
  def parse(sql: String): ParseResult = {
    val result = squid.parser.jni.PGParserJNI.nativeParse(sql)
    if (result.error != null) {
      return ParseError(result.error.message)
    }
    if (result.parseTree == null) {
      throw new RuntimeException("Expected parseTree to be not null")
    }
    argonaut.Parse.parse(result.parseTree).fold(
      ParseError,
      json => json.as[List[SQL]].fold(
        (err, history) => ParseError(s"$err\nCursorHistory: $history"),
        {
          case Nil => ParseError("Empty json array")
          case x :: Nil => ParseSuccess(x)
          case _ => ParseError("Only one SQL statement is supported.")
        }
      )
    )
  }

  def unsafeParse(sql: String): SQL = {
    parse(sql).fold(
      err => throw new RuntimeException(err),
      identity
    )
  }

  sealed trait ParseResult {
    def fold[X](left: String => X, right: SQL => X): X = this match {
      case ParseError(err) => left(err)
      case ParseSuccess(sql) => right(sql)
    }
  }
  sealed case class ParseError(message: String) extends ParseResult
  sealed case class ParseSuccess(result: SQL) extends ParseResult

  private lazy val config = ConfigFactory.load(getClass.getClassLoader)
  private lazy val parser = config.getString("squid.parser")
  private lazy val command = List(parser)
}
