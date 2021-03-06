package squid.parser

import scala.util.parsing.combinator.RegexParsers

/** Parser for consuming PostgreSQL SQL ASTs */
object PGParser extends RegexParsers {

  // Whitespace is insignificant in the AST.
  override val skipWhitespace = true

  /** Parse a string to a raw Atom. */
  def parse(s: String): ParseResult[Atom] = parse(obj, s)

  /** Values parsed from a PostgreSQL AST. */
  sealed trait Atom
  object Atom {
    case object Null extends Atom
    final case class Int(value: scala.Int) extends Atom
    final case class Bool(value: Boolean) extends Atom
    final case class Ident(value: String) extends Atom
    case object MissingIdent extends Atom
    final case class Datum(size: Int, bytes: scala.List[Int]) extends Atom
    final case class List(value: scala.List[Atom]) extends Atom
    final case class Obj(name: String, value: scala.List[(String, Atom)]) extends Atom {
      lazy val toMap = value.toMap
    }
  }

  def obj: Parser[Atom] = for {
    name <- objOpen
    kvps <- rep1(kvp)
    _ <- objClose
  } yield Atom.Obj(name, kvps)

  def kvp: Parser[(String, Atom)] = for {
    k <- key
    v <- atom
  } yield (k, v)

  def atom: Parser[Atom] = bool | nul | datum | int | ident | obj | list

  // TODO: For some reason the parser isn't skipping spaces here, so have to explicitly use them.
  def datum: Parser[Atom] = for {
    size <- int
    _ <- ' '
    _ <- '['
    bytes <- rep(int)
    _ <- ' '
    _ <- ']'
  } yield {
    Atom.Datum(size, bytes)
  }

  def aChar = for {
    c <- ".".r
  } yield {
    println(s"found: '$c'")
    println(s"char code: '${c.chars().toArray.toList}'")
    c
  }

  def list: Parser[Atom] = for {
    _ <- listOpen
    xs <- rep1(atom)
    _ <- listClose
  } yield Atom.List(xs)

  /** Apply a parser but ignore its result. */
  def void(p: Parser[_]): Parser[Unit] = p.map(_ => ())

  val bool = (
    literal("true").map(_ => true) | literal("false").map(_ => false)
  ).map(Atom.Bool)

  val nul = literal("<>").map(_ => Atom.Null)
  val int = regex("(\\+|\\-)?\\d+".r).map(s => Atom.Int(s.toInt))
  val unquotedIdent = regex("\\w+".r)
  val quotedIdent = regex("\"[^\"]+\"".r).map(_.init.tail)
  val missingIdent = literal("?column?")
  val ident =
    (unquotedIdent | quotedIdent).map(Atom.Ident) | missingIdent.map(_ => Atom.MissingIdent)
  val key = regex(":\\w+".r).map(_.tail)
  val objOpen = regex("\\{\\w+".r).map(_.tail)
  val objClose = void(literal("}"))
  val listOpen = void(literal("("))
  val listClose = void(literal(")"))
}
