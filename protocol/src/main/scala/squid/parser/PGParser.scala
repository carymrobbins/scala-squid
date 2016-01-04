package squid.parser

import scala.util.parsing.combinator.RegexParsers

import squid.protocol.OID

/**
  * Parser for consuming PostgreSQL SQL ASTs
  */
object PGParser extends RegexParsers {
  override val skipWhitespace = true

  def parse(s: String): ParseResult[Query] = parse(query, s)

  def query: Parser[Query] = for {
    _ <- literal("{QUERY")
    m <- kvps
    cmdType <- getKey(m, Keys.commandType)
    _ <- elem('}')
  } yield Query(cmdType)

  def getKey[A](m: HMap, k: Key[A]): Parser[A] = {
    m.get(k) match {
      case Some(v) => success(v)
      case None => failure(s"Expected key ${k.key}")
    }
  }

  def kvps: Parser[HMap] = {
    val m = new HMap(Map.empty)
    rep1(kvp(m)).map(HMap.join)
  }

  def kvp(m: HMap): Parser[HMap] = {

    def ret[A](k: Key[A], p: Parser[A]) = p.map(v => m.put(k, v))

    key.flatMap {
      case Keys.commandType.Matched(k) => ret(k, commandType)
      case Keys.querySource.Matched(k) => ret(k, querySource)
      case Keys.canSetTag.Matched(k) => ret(k, bool)
      case Keys.resultRelation.Matched(k) => ret(k, int)
      case Keys.hasAggs.Matched(k) => ret(k, bool)
      case Keys.hasWindowFuncs.Matched(k) => ret(k, bool)
      case Keys.hasSubLinks.Matched(k) => ret(k, bool)
      case Keys.hasDistinctOn.Matched(k) => ret(k, bool)
      case Keys.hasRecursive.Matched(k) => ret(k, bool)
      case Keys.hasModifyingCTE.Matched(k) => ret(k, bool)
      case Keys.hasForUpdate.Matched(k) => ret(k, bool)
      // TODO case Keys.rtable.Matched(k) => ret(k, rtable)
      case k => failure(s"Unknown key: $k")
    }
  }

  def commandType = for {
    n <- int
    v <- fromOpt(CmdType.up(n), s"Invalid CmdType: $n")
  } yield v

  def querySource = for {
    n <- int
    v <- fromOpt(QuerySource.up(n), s"Invalid QuerySource: $n")
  } yield v

  def key: Parser[String] = regex(REGEX.KEY).map(_.tail.toString)

  def int: Parser[Int] = regex(REGEX.UINT).map(_.toInt)

  def bool: Parser[Boolean] = literal("true").map(_ => true) | literal("false").map(_ => false)

  def nul: Parser[Option[Nothing]] = literal("<>").map(_ => None)

  def nullable[A](p: Parser[A]): Parser[Option[A]] = p.map(Some(_)) | nul

  def fromOpt[A](o: Option[A], err: String): Parser[A] = o match {
    case Some(x) => success(x)
    case None => failure(err)
  }

  object REGEX {
    val KEY = ":\\w+".r
    val UINT = "\\d+".r
  }
}

case class Key[A](key: String) { self =>
  object Matched {
    def unapply(s: String): Option[self.type] = if (s == key) Some(self) else None
  }
}

final class HMap(val underlying: Map[Any, Any]) extends AnyVal {
  def get[A](k: Key[A]): Option[A] = underlying.get(k).map(_.asInstanceOf[A])
  def put[A](k: Key[A], v: A): HMap = new HMap(underlying.updated(k, v))
}

object HMap {
  // TODO: This is a hack, how can we do this better but still safely?
  def join(ms: List[HMap]): HMap = new HMap(ms.flatMap(_.underlying).toMap)
}

object Keys {
  val commandType = Key[CmdType]("commandType")
  val querySource = Key[QuerySource]("querySource")
  val canSetTag = Key[Boolean]("canSetTag")
  val resultRelation = Key[Int]("resultRelation")
  val hasAggs = Key[Boolean]("hasAggs")
  val hasWindowFuncs = Key[Boolean]("hasWindowFuncs")
  val hasSubLinks = Key[Boolean]("hasSubLinks")
  val hasDistinctOn = Key[Boolean]("hasDistinctOn")
  val hasRecursive = Key[Boolean]("hasRecursive")
  val hasModifyingCTE = Key[Boolean]("hasModifyingCTE")
  val hasForUpdate = Key[Boolean]("hasForUpdate")
  val rtable = Key[RangeTblEntry]("rtable")
}

/**
  * Structure of the PostgreSQL Query AST
  * PostgreSQL Documentation: http://doxygen.postgresql.org/structQuery.html
  */
final case class Query(
  commandType: CmdType
)

trait Iso[A] {
  def down: A
}

trait IsoCompanion[A <: Iso[B], B] {
  def values: List[A]

  def up(n: B): Option[A] = {
    values.collectFirst { case x if x.down == n => x }
  }
}

sealed abstract class CmdType(val down: Int) extends Iso[Int]
object CmdType extends IsoCompanion[CmdType, Int] {
  case object Unknown extends CmdType(0)
  case object Select extends CmdType(1)
  case object Update extends CmdType(2)
  case object Insert extends CmdType(3)
  case object Delete extends CmdType(4)
  case object Utility extends CmdType(5)
  case object Nothing extends CmdType(6)

  val values = List(
    Unknown, Select, Update, Insert, Delete, Utility, Nothing
  )
}

sealed abstract class QuerySource(val down: Int) extends Iso[Int]
object QuerySource extends IsoCompanion[QuerySource, Int] {
  case object Original extends QuerySource(0)
  case object Parser extends QuerySource(1)
  case object InsteadRule extends QuerySource(2)
  case object QualInsteadRule extends QuerySource(3)
  case object NonInsteadRule extends QuerySource(4)

  val values = List(
    Original, Parser, InsteadRule, QualInsteadRule, NonInsteadRule
  )
}

sealed case class RangeTblEntry(
  alias: Option[Alias],
  eref: Option[Alias],
  rtekind: RTEKind,
  relid: OID,
  relkind: RelKind,
  lateral: Boolean,
  inh: Boolean,
  inFromCl: Boolean
)

sealed abstract class RTEKind(val down: Int) extends Iso[Int]
object RTEKind extends IsoCompanion[RTEKind, Int] {
  case object Relation extends RTEKind(0)
  case object SubQuery extends RTEKind(1)
  case object Join extends RTEKind(2)
  case object Function extends RTEKind(3)
  case object Values extends RTEKind(4)
  case object CTE extends RTEKind(5)

  val values = List(
    Relation, SubQuery, Join, Function, Values, CTE
  )
}

sealed abstract class RelKind(val down: Char) extends Iso[Char]
object RelKind extends IsoCompanion[RelKind, Char] {
  case object Table extends RelKind('r')
  case object Index extends RelKind('i')
  case object Sequence extends RelKind('S')
  case object View extends RelKind('V')
  case object CompositeType extends RelKind('C')
  case object ToastTable extends RelKind('t')

  val values = List(
    Table, Index, Sequence, View, CompositeType, ToastTable
  )
}

sealed case class Alias(
  aliasname: String,
  colnames: Option[List[String]] = None
)
