package squid.parser

import squid.protocol.OID

import PGParser.Expr

/**
  * Structure of the PostgreSQL Query AST
  * PostgreSQL Documentation: http://doxygen.postgresql.org/structQuery.html
  */
final case class Query(
  commandType: CmdType,
  querySource: QuerySource,
  canSetTag: Boolean,
  utilityStmt: Option[Nothing],
  resultRelation: Int,
  hasAggs: Boolean,
  hasWindowFuncs: Boolean,
  hasSubLinks: Boolean,
  hasDistinctOn: Boolean,
  hasRecursive: Boolean,
  hasModifyingCTE: Boolean,
  hasForUpdate: Boolean,
  cteList: Option[Nothing]
  // rtable: RangeTblEntry,
  //  jointree: ,
  //  targetList: ,
  //  withCheckOptions: ,
  //  returningList: ,
  //  groupClause: ,
  //  havingQual: ,
  //  windowClause: ,
  //  distinctClause: ,
  //  sortClause: ,
  //  limitOffset: ,
  //  limitCount: ,
  //  rowMarks: ,
  //  setOperations: ,
  //  constraintDeps:
)

object Query {
  def fromExpr(expr: Expr): PGParseDecode[Query] = {
    import PGParseDecode.Combinators._
    for {
      o <- obj(expr, "QUERY")
      commandType <- get(o, "commandType", int).flatMap(iso(CmdType, _))
      querySource <- get(o, "querySource", int).flatMap(iso(QuerySource, _))
      canSetTag <- get(o, "canSetTag", bool)
      utilityStmt <- get(o, "utilityStmt", nul)
      resultRelation <- get(o, "resultRelation", int)
      hasAggs <- get(o, "hasAggs", bool)
      hasWindowFuncs <- get(o, "hasWindowFuncs", bool)
      hasSubLinks <- get(o, "hasSubLinks", bool)
      hasDistinctOn <- get(o, "hasDistinctOn", bool)
      hasRecursive <- get(o, "hasRecursive", bool)
      hasModifyingCTE <- get(o, "hasModifyingCTE", bool)
      hasForUpdate <- get(o, "hasForUpdate", bool)
    } yield Query(
      commandType = commandType,
      querySource = querySource,
      canSetTag = canSetTag,
      utilityStmt = utilityStmt,
      resultRelation = resultRelation,
      hasAggs = hasAggs,
      hasWindowFuncs = hasWindowFuncs,
      hasSubLinks = hasSubLinks,
      hasDistinctOn = hasDistinctOn,
      hasRecursive = hasRecursive,
      hasModifyingCTE = hasModifyingCTE,
      hasForUpdate = hasForUpdate,
      cteList = None
    )
  }
}

sealed trait PGParseDecode[+A] {
  def fold[X](failure: String => X, success: A => X): X = this match {
    case PGParseDecode.Failure(msg) => failure(msg)
    case PGParseDecode.Success(v) => success(v)
  }

  def map[B](f: A => B): PGParseDecode[B] = this match {
    case e: PGParseDecode.Failure => e
    case PGParseDecode.Success(v) => PGParseDecode.Success(f(v))
  }

  def flatMap[B](f: A => PGParseDecode[B]): PGParseDecode[B] = this match {
    case e: PGParseDecode.Failure => e
    case PGParseDecode.Success(v) => f(v)
  }

  def leftMap(f: String => String): PGParseDecode[A] = this match {
    case e: PGParseDecode.Failure => PGParseDecode.Failure(f(e.message))
    case PGParseDecode.Success(v) => PGParseDecode.Success(v)
  }
}

object PGParseDecode {
  final case class Failure(message: String) extends PGParseDecode[Nothing]
  final case class Success[A](value: A) extends PGParseDecode[A]

  object Combinators {
    def obj(expr: Expr, name: String): PGParseDecode[Map[String, Expr]] = {
      expr match {
        case o: Expr.Obj =>
          if (o.name == name) {
            PGParseDecode.Success(o.toMap)
          } else {
            PGParseDecode.Failure(s"Expected object name '$name' but got '${o.name}'")
          }

        case other => PGParseDecode.Failure(s"Expected object, got: $other")
      }
    }

    def get(m: Map[String, Expr], k: String): PGParseDecode[Expr] = {
      m.get(k) match {
        case None => PGParseDecode.Failure(s"Key not found '$k'")
        case Some(v) => PGParseDecode.Success(v)
      }
    }

    def get[A](m: Map[String, Expr], k: String, f: Expr => PGParseDecode[A]): PGParseDecode[A] = {
      get(m, k).flatMap(expr => f(expr).leftMap(e => s"For key '$k', $e"))
    }

    def int(expr: Expr): PGParseDecode[Int] = expr match {
      case Expr.Int(n) => PGParseDecode.Success(n)
      case other => PGParseDecode.Failure(s"Expected int, got: $other")
    }

    def bool(expr: Expr): PGParseDecode[Boolean] = expr match {
      case Expr.Bool(b) => PGParseDecode.Success(b)
      case other => PGParseDecode.Failure(s"Expected bool, got: $other")
    }

    def nul(expr: Expr): PGParseDecode[Option[Nothing]] = expr match {
      case Expr.Null => PGParseDecode.Success(None)
      case other => PGParseDecode.Failure(s"Expected null, got: $other")
    }

    def iso[A <: Iso[K], K](c: IsoCompanion[A, K], k: K): PGParseDecode[A] = c.up(k) match {
      case None => PGParseDecode.Failure(s"Invalid $c value '$k'")
      case Some(v) => PGParseDecode.Success(v)
    }
  }
}

/** Helper to convert to/from some enum-like value. */
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

