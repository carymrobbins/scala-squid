package squid.parser

import scala.util.parsing.combinator.RegexParsers

/** Parser for consuming PostgreSQL SQL ASTs */
object PGParser extends RegexParsers {
  override val skipWhitespace = true

  def parse(s: String): ParseResult[Expr] = parse(obj, s)

  def obj: Parser[Expr] = for {
    name <- objOpen
    kvps <- rep1(kvp)
    _ <- objClose
  } yield Expr.Obj(name, kvps)

  def kvp: Parser[(String, Expr)] = for {
    k <- key
    v <- expr
  } yield (k, v)

  def expr: Parser[Expr] = bool | nul | int | ident | obj | tuple

  def tuple: Parser[Expr] = for {
    _ <- tupleOpen
    xs <- rep1(expr)
    _ <- tupleClose
  } yield Expr.Tuple(xs)

  def void(p: Parser[_]): Parser[Unit] = p.map(_ => ())

  val bool = (
    literal("true").map(_ => true) | literal("false").map(_ => false)
  ).map(Expr.Bool)

  val nul = literal("<>").map(_ => Expr.Null)
  val int = regex("(\\+|\\-)?\\d+".r).map(s => Expr.Int(s.toInt))
  val unquotedIdent = regex("\\w+".r)
  val quotedIdent = regex("\"[^\"]+\"".r).map(_.init.tail)
  val ident = (unquotedIdent | quotedIdent).map(Expr.Ident)
  val key = regex(":\\w+".r).map(_.tail)
  val objOpen = regex("\\{\\w+".r).map(_.tail)
  val objClose = void(literal("}"))
  val tupleOpen = void(literal("("))
  val tupleClose = void(literal(")"))

  sealed trait Expr
  object Expr {
    case object Null extends Expr
    final case class Int(value: scala.Int) extends Expr
    final case class Bool(value: Boolean) extends Expr
    final case class Ident(value: String) extends Expr
    final case class Tuple(value: List[Expr]) extends Expr
    final case class Obj(name: String, value: List[(String, Expr)]) extends Expr {
      lazy val toMap = value.toMap
    }
  }
}
