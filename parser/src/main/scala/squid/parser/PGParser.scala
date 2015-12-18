package squid.parser

import argonaut.CursorHistory
import squid.parser.ast.SQL
import squid.parser.jni.PGParserJNI

/** Consumes output from queryparser */
object PGParser {
  def parse(sql: String): ParseResult = {
    val result = PGParserJNI.nativeParse(sql)
    if (result.error != null) return ParseResult.fromPGParseError(result.error)
    if (result.parseTree == null) throw new RuntimeException("Expected parseTree to be not null")
    argonaut.Parse.parse(result.parseTree).fold(
      JsonParseError,
      json => json.array match {
        case Some(List(x)) => x.as[SQL].fold(JsonDeserializationError, ParseSuccess)
        case _ => JsonParseError(s"Expected a singleton JSON array, got: $json")
      }
    )
  }

  def unsafeParse(sql: String): SQL = {
    parse(sql).fold(
      err => throw new RuntimeException(err.toString),
      identity
    )
  }

  sealed trait ParseResult {
    def fold[X](left: ParseError => X, right: SQL => X): X = this match {
      case err: ParseError => left(err)
      case ParseSuccess(sql) => right(sql)
    }
  }

  sealed trait ParseError extends ParseResult

  object ParseResult {
    def fromPGParseError(error: PGParserJNI.PGParseError): SQLParseError = {
      SQLParseError(message = error.message, line = error.line, cursorPos = error.cursorPos)
    }
  }

  sealed case class SQLParseError(
    message: String, line: Int, cursorPos: Int
  ) extends ParseError

  sealed case class JsonParseError(message: String) extends ParseError

  sealed case class JsonDeserializationError(
    message: String, history: CursorHistory
  ) extends ParseError

  sealed case class UnsupportedSQLError(message: String) extends ParseError

  sealed case class ParseSuccess(result: SQL) extends ParseResult
}
