package squid.parser

import com.typesafe.config.ConfigFactory
import squid.parser.ast.SQL

/** Consumes output from queryparser */
object PGParser {
  def parse(sql: String): ParseResult = {
    val parsed = squid.parser.jni.PGParserJNI.nativeParse(sql)
    // TODO: Have JNI return an object instead of this hack.
    if (parsed.startsWith("error")) {
      ParseError(parsed)
    } else {
      argonaut.Parse.parse(parsed).fold(
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
