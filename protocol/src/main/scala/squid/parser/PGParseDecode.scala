package squid.parser

import squid.protocol.OID

import PGParser.Atom
import PGParseDecode.Combinators._

/**
  * Describes the result of of decoding a value from a PGParser.Atom
  * There are two constructors: Failure and Success.
  * This trait can be seen as an analog of Either.
  */
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

  def orElse[AA >: A](other: => PGParseDecode[AA]): PGParseDecode[AA] = this match {
    case PGParseDecode.Success(v) => PGParseDecode.Success(v)
    case _: PGParseDecode.Failure => other
  }

  /** Alias for .orElse */
  def |[AA >: A](other: => PGParseDecode[AA]): PGParseDecode[AA] = orElse(other)
}

object PGParseDecode {
  final case class Failure(message: String) extends PGParseDecode[Nothing]
  final case class Success[A](value: A) extends PGParseDecode[A]

  /**
    * Helpful combinators for decoding a value from a PGParser.Atom
    * These are imported at the top of this file to simplify decoding logic.
    */
  object Combinators {
    /** Decodes an object */
    def obj(atom: Atom, name: String): PGParseDecode[Map[String, Atom]] = {
      atom match {
        case o: Atom.Obj =>
          if (o.name == name) {
            PGParseDecode.Success(o.toMap)
          } else {
            PGParseDecode.Failure(s"Expected object name '$name' but got '${o.name}'")
          }

        case other => PGParseDecode.Failure(s"Expected object, got: $other")
      }
    }

    /** Gets an Atom from a Map requiring the given key. */
    def get(m: Map[String, Atom], k: String): PGParseDecode[Atom] = {
      m.get(k) match {
        case None => PGParseDecode.Failure(s"Key not found '$k'")
        case Some(v) => PGParseDecode.Success(v)
      }
    }

    /** Same as .get above except also decodes the Atom found. */
    def get[A](m: Map[String, Atom], k: String, f: Atom => PGParseDecode[A]): PGParseDecode[A] = {
      get(m, k).flatMap(atom => f(atom).leftMap(e => s"For key '$k', $e"))
    }

    /** Decodes an int */
    def int(atom: Atom): PGParseDecode[Int] = atom match {
      case Atom.Int(n) => PGParseDecode.Success(n)
      case other => PGParseDecode.Failure(s"Expected int, got: $other")
    }

    /** Decodes a boolean */
    def bool(atom: Atom): PGParseDecode[Boolean] = atom match {
      case Atom.Bool(b) => PGParseDecode.Success(b)
      case other => PGParseDecode.Failure(s"Expected bool, got: $other")
    }

    /** Decodes an identifier */
    def ident(atom: Atom): PGParseDecode[String] = atom match {
      case Atom.Ident(s) => PGParseDecode.Success(s)
      case other => PGParseDecode.Failure(s"Expected ident, got: $other")
    }

    /** Decodes an OID */
    def oid(atom: Atom): PGParseDecode[OID] = atom match {
      case Atom.Int(n) if n >= 0 => PGParseDecode.Success(OID(n))
      case other => PGParseDecode.Failure(s"Expected oid, got: $other")
    }

    /** Decodes a single character, failing if more than one character is found */
    def char(atom: Atom): PGParseDecode[Char] = atom match {
      case Atom.Ident(s) if s.length == 1 => PGParseDecode.Success(s.head)
      case other => PGParseDecode.Failure(s"Expected char, got: $other")
    }

    /** Repeatedly applies a decoding function within a list atom. */
    def list[A](f: Atom => PGParseDecode[A])(atom: Atom): PGParseDecode[List[A]] = atom match {
      case Atom.List(xs) =>
        xs match {
          case Nil => PGParseDecode.Success(Nil)
          case head :: tail => tail.foldRight(f(head).map(List(_)))((x, acc) =>
            acc.flatMap(ys => f(x).map(ys :+ _))
          )
        }

      case other => PGParseDecode.Failure(s"Expected list, got: $other")
    }

    /** Returns None for a null atom, otherwise applies a decoding function. */
    def nullable[A](f: Atom => PGParseDecode[A])(atom: Atom): PGParseDecode[Option[A]] = {
      atom match {
        case Atom.Null => PGParseDecode.Success(None)
        case _ => f(atom).map(Some(_))
      }
    }

    /** Decode a null atom to None, failing for non-null atoms. */
    def nul(atom: Atom): PGParseDecode[Option[Nothing]] = atom match {
      case Atom.Null => PGParseDecode.Success(None)
      case other => PGParseDecode.Failure(s"Expected null, got: $other")
    }

    /** Helper to convert Iso values to their complementary value. */
    def iso[A <: PartialIso[K], K](c: PartialIsoCompanion[A, K])(k: K): PGParseDecode[A] = c.up(k) match {
      case None => PGParseDecode.Failure(s"Invalid $c value '$k'")
      case Some(v) => PGParseDecode.Success(v)
    }
  }
}

// Below are the various AST classes decoded into usable data structures.
// NOTE: Many fields of the decoded AST classes have been left commented out.  These fields
// are not very useful to us and introducing them makes testing the parse trees harder.
// They are left in the source as a way of identifying fields that may be introduced later
// if need be.

/**
  * Structure of the PostgreSQL Query AST
  * PostgreSQL Documentation: http://doxygen.postgresql.org/structQuery.html
  */
final case class Query(
  commandType: CmdType,
  querySource: QuerySource,
  canSetTag: Boolean,
  // utilityStmt: Option[Nothing],
  resultRelation: Int,
  hasAggs: Boolean,
  hasWindowFuncs: Boolean,
  hasSubLinks: Boolean,
  hasDistinctOn: Boolean,
  hasRecursive: Boolean,
  hasModifyingCTE: Boolean,
  hasForUpdate: Boolean,
  cteList: Option[Nothing],
  rtable: List[RangeTblEntry],
  jointree: FromExpr,
  targetList: List[TargetEntry],
  withCheckOptions:  Option[Nothing],
  returningList: Option[Nothing],
  groupClause: Option[Nothing],
  havingQual: Option[Nothing],
  windowClause: Option[Nothing],
  distinctClause: Option[Nothing],
  sortClause: Option[Nothing],
  limitOffset: Option[Nothing],
  limitCount: Option[Nothing],
  rowMarks: Option[Nothing],
  setOperations: Option[Nothing],
  constraintDeps: Option[Nothing]
)

object Query {
  def decode(atom: Atom): PGParseDecode[Query] = {
    for {
      o <- obj(atom, "QUERY")
      commandType <- get(o, "commandType", int).flatMap(iso(CmdType))
      querySource <- get(o, "querySource", int).flatMap(iso(QuerySource))
      canSetTag <- get(o, "canSetTag", bool)
      resultRelation <- get(o, "resultRelation", int)
      hasAggs <- get(o, "hasAggs", bool)
      hasWindowFuncs <- get(o, "hasWindowFuncs", bool)
      hasSubLinks <- get(o, "hasSubLinks", bool)
      hasDistinctOn <- get(o, "hasDistinctOn", bool)
      hasRecursive <- get(o, "hasRecursive", bool)
      hasModifyingCTE <- get(o, "hasModifyingCTE", bool)
      hasForUpdate <- get(o, "hasForUpdate", bool)
      rtable <- get(o, "rtable", list(RangeTblEntry.decode))
      jointree <- get(o, "jointree", FromExpr.decode)
      targetList <- get(o, "targetList", list(TargetEntry.decode))
      withCheckOptions <- get(o, "withCheckOptions", nul)
      returningList <- get(o, "returningList", nul)
      groupClause <- get(o, "groupClause", nul)
      havingQual <- get(o, "havingQual", nul)
      windowClause <- get(o, "windowClause", nul)
      distinctClause <- get(o, "distinctClause", nul)
      sortClause <- get(o, "sortClause", nul)
      limitOffset <- get(o, "limitOffset", nul)
      limitCount <- get(o, "limitCount", nul)
      rowMarks <- get(o, "rowMarks", nul)
      setOperations <- get(o, "setOperations", nul)
      constraintDeps <- get(o, "constraintDeps", nul)
    } yield Query(
      commandType = commandType,
      querySource = querySource,
      canSetTag = canSetTag,
      // utilityStmt = utilityStmt,
      resultRelation = resultRelation,
      hasAggs = hasAggs,
      hasWindowFuncs = hasWindowFuncs,
      hasSubLinks = hasSubLinks,
      hasDistinctOn = hasDistinctOn,
      hasRecursive = hasRecursive,
      hasModifyingCTE = hasModifyingCTE,
      hasForUpdate = hasForUpdate,
      cteList = None,
      rtable = rtable,
      jointree = jointree,
      targetList = targetList,
      withCheckOptions = withCheckOptions,
      returningList = returningList,
      groupClause = groupClause,
      havingQual = havingQual,
      windowClause = windowClause,
      distinctClause = distinctClause,
      sortClause = sortClause,
      limitOffset = limitOffset,
      limitCount = limitCount,
      rowMarks = rowMarks,
      setOperations = setOperations,
      constraintDeps = constraintDeps
    )
  }
}

/**
  * Trait for partial isomorphisms.
  * In other words, for any given Iso[A] we can get an A.
  * Conversely, for only some A can we get an Iso[A].
  * While this could be defined as a typeclass, for our uses this is actually simpler.
  */
trait PartialIso[A] {
  def down: A
}

/** Trait for companion to an Iso[A] to simplify going from A to Iso[A]. */
trait PartialIsoCompanion[A <: PartialIso[B], B] {
  def values: List[A]

  def up(n: B): Option[A] = {
    values.collectFirst { case x if x.down == n => x }
  }
}

sealed abstract class CmdType(val down: Int) extends PartialIso[Int]
object CmdType extends PartialIsoCompanion[CmdType, Int] {
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

sealed abstract class QuerySource(val down: Int) extends PartialIso[Int]
object QuerySource extends PartialIsoCompanion[QuerySource, Int] {
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
  // requiredPerms
  // checkAsUser
  // selectedCols
  // modifiedCols
  // securityQuals
)

object RangeTblEntry {
  def decode(atom: Atom): PGParseDecode[RangeTblEntry] = for {
    o <- obj(atom, "RTE")
    alias <- get(o, "alias", nullable(Alias.decode))
    eref <- get(o, "eref", nullable(Alias.decode))
    rtekind <- get(o, "rtekind", int).flatMap(iso(RTEKind))
    relid <- get(o, "relid", oid)
    relkind <- get(o, "relkind", char).flatMap(iso(RelKind))
    lateral <- get(o, "lateral", bool)
    inh <- get(o, "inh", bool)
    inFromCl <- get(o, "inFromCl", bool)
  } yield RangeTblEntry(
    alias = alias,
    eref = eref,
    rtekind = rtekind,
    relid = relid,
    relkind = relkind,
    lateral = lateral,
    inh = inh,
    inFromCl = inFromCl
  )
}

sealed abstract class RTEKind(val down: Int) extends PartialIso[Int]
object RTEKind extends PartialIsoCompanion[RTEKind, Int] {
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

sealed abstract class RelKind(val down: Char) extends PartialIso[Char]
object RelKind extends PartialIsoCompanion[RelKind, Char] {
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

final case class Alias(
  aliasname: String,
  colnames: Option[List[String]] = None
)

object Alias {
  def decode(atom: Atom): PGParseDecode[Alias] = for {
    o <- obj(atom, "ALIAS")
    aliasname <- get(o, "aliasname", ident)
    colnames <- get(o, "colnames", nullable(list(ident)))
  } yield Alias(
    aliasname = aliasname,
    colnames = colnames
  )
}

final case class FromExpr(
  fromlist: List[RangeTblRef],
  quals: Option[Nothing]
)

object FromExpr {
  def decode(atom: Atom): PGParseDecode[FromExpr] = for {
    o <- obj(atom, "FROMEXPR")
    fromlist <- get(o, "fromlist", list(RangeTblRef.decode))
    quals <- get(o, "quals", nul)
  } yield FromExpr(
    fromlist = fromlist,
    quals = quals
  )
}

final case class RangeTblRef(
  rtindex: Int
)

object RangeTblRef {
  def decode(atom: Atom): PGParseDecode[RangeTblRef] = for {
    o <- obj(atom, "RANGETBLREF")
    rtindex <- get(o, "rtindex", int)
  } yield RangeTblRef(
    rtindex = rtindex
  )
}

final case class TargetEntry(
  expr: Expr,
  resno: Int,
  resname: Option[String],
  ressortgroupref: Int,
  resorigtbl: OID,
  resorigcol: Int
  // resjunk: Boolean
)

object TargetEntry {
  def decode(atom: Atom): PGParseDecode[TargetEntry] = for {
    o <- obj(atom, "TARGETENTRY")
    expr <- get(o, "expr", Expr.decode)
    resno <- get(o, "resno", int)
    resname <- get(o, "resname", nullable(ident))
    ressortgroupref <- get(o, "ressortgroupref", int)
    resorigtbl <- get(o, "resorigtbl", oid)
    resorigcol <- get(o, "resorigcol", int)
    // resjunk <- get(o, "resjunk", bool)
  } yield TargetEntry(
    expr = expr,
    resno = resno,
    resname = resname,
    ressortgroupref = ressortgroupref,
    resorigtbl = resorigtbl,
    resorigcol = resorigcol
    // resjunk = resjunk
  )
}

sealed trait Expr

object Expr {
  def decode(atom: Atom): PGParseDecode[Expr] = {
    Var.decode(atom)
  }
}

sealed case class Var(
  varno: Int,
  varattno: Int,
  vartype: OID,
  vartypmod: Int,
  // varcollid: Int,
  varlevelsup: Int
  // varnoold: Int,
  // varoattno: Int,
  // location: Int
) extends Expr

object Var {
  def decode(atom: Atom): PGParseDecode[Var] = for {
    o <- obj(atom, "VAR")
    varno <- get(o, "varno", int)
    varattno <- get(o, "varattno", int)
    vartype <- get(o, "vartype", oid)
    vartypmod <- get(o, "vartypmod", int)
    // varcollid <- get(o, "varcollid", int)
    varlevelsup <- get(o, "varlevelsup", int)
    // varnoold <- get(o, "varnoold", int)
    // varoattno <- get(o, "varoattno", int)
    // location <- get(o, "location", int)
  } yield Var(
    varno = varno,
    varattno = varattno,
    vartype = vartype,
    vartypmod = vartypmod,
    // varcollid = varcollid,
    varlevelsup = varlevelsup
    // varnoold = varnoold,
    // varoattno = varoattno,
    // location = location
  )
}
