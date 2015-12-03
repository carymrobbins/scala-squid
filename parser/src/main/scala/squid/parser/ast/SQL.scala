package squid.parser.ast

import argonaut._
import Argonaut._

object JsonHelpers {
  def tagged[A](tag: JsonField, c: HCursor, decoder: DecodeJson[_ <: A]): DecodeResult[A] = {
    c.downField(tag).hcursor.fold(
      DecodeResult.fail[A](s"""No "$tag" key found""", c.history)
    )(c => decoder.decode(c).map(x => x: A))
  }

  def tagged[A](tag: JsonField, decoder: DecodeJson[_ <: A]): DecodeJson[A] = DecodeJson(c =>
    tagged(tag, c, decoder)
  )

  def fail[A](err: String): DecodeJson[A] = DecodeJson(c =>
    DecodeResult.fail(s"$err\nJSON: ${c.focus}", c.history)
  )
}
import JsonHelpers._

// The postgres source is the best place to look for the AST information
// https://github.com/postgres/postgres/tree/master/src/include/nodes

sealed trait SQL

object SQL {
  implicit def sqlDecodeJson: DecodeJson[SQL] = DecodeJson(c =>
    tagged("SELECT", c, DecodeJson.derive[Select])
  )
}

sealed case class Select(
  targetList: List[ResTarget],
  all: Boolean = false,
  distinctClause: Option[List[Expr]] = None,
  fromClause: Option[List[RangeRef]] = None,
  groupClause: Option[List[Expr]] = None,
  havingClause: Option[List[Expr]] = None,
  limitCount: Option[Expr] = None,
  limitOffset: Option[Expr] = None,
  sortClause: Option[List[SortBy]] = None,
  valuesLists: Option[List[Expr]] = None,
  whereClause: Option[Expr] = None,
  windowClause: Option[List[WindowClause]] = None,
  withClause: Option[WithClause] = None
  // intoClause,
  // larg,
  // lockingClause,
  // op,
  // rarg,
) extends SQL

sealed trait Expr

case object ConstNull extends Expr
sealed case class ConstInt(`val`: Long) extends Expr
sealed case class ConstDecimal(`val`: BigDecimal) extends Expr
sealed case class ConstString(`val`: String) extends Expr

sealed case class TypeCast(
  arg: Expr,
  typeName: TypeName
) extends Expr

sealed case class ColumnRef(
  fields: List[FieldPart]
) extends Expr

object ColumnRef {
  def named(name1: String, names: String*): ColumnRef = {
    ColumnRef((name1 +: names).map(FieldPart.Name).toList)
  }
}

sealed trait FieldPart
object FieldPart {
  sealed case class Name(name: String) extends FieldPart
  case object Star extends FieldPart

  implicit def fieldPartDecodeJson: DecodeJson[FieldPart] = name ||| star

  private def name = implicitly[DecodeJson[String]].map(s => Name(s): FieldPart)

  private def star = tagged[FieldPart]("A_STAR", implicitly[DecodeJson[Unit]].map(_ => Star))
}

sealed case class AExpr(
  lexpr: Expr,
  name: List[String],
  rexpr: Expr
) extends Expr

sealed case class AExprAnd(lexpr: Expr, rexpr: Expr) extends Expr

sealed case class AExprOr(lexpr: Expr, rexpr: Expr) extends Expr

sealed case class AExprNot(rexpr: Expr) extends Expr

sealed case class FuncCall(
  funcname: List[String],
  args: Option[List[Expr]] = None,
  agg_order: Option[List[SortBy]] = None,
  agg_filter: Option[Expr] = None,
  agg_within_group: Boolean = false,
  agg_star: Boolean = false,
  agg_distinct: Boolean = false,
  func_variadic: Boolean = false,
  over: Option[WindowClause] = None
) extends Expr

sealed case class Coalesce(args: List[Expr]) extends Expr

sealed case class ParamRef(number: Int) extends Expr

object Expr {
  implicit def exprDecodeJson: DecodeJson[Expr] = (
    const ||| typeCast ||| columnRef ||| funcCall ||| paramRef
      ||| aExpr ||| aExprAnd ||| aExprOr ||| aExprNot ||| coalesce
      ||| fail("Failed to parse Expr")
    )

  private def const: DecodeJson[Expr] =
    nullExpr ||| constInt ||| constDecimal ||| constString

  private def constValDecoder[A <: Expr](f: Json => Option[A], err: String): DecodeJson[Expr] =
    DecodeJson(c =>
      c.downField("A_CONST").downField("val").focus.flatMap(f)
        .map(x => DecodeResult.ok(x: Expr))
        .getOrElse(DecodeResult.fail(err, c.history))
    )

  private def nullExpr = constValDecoder(
    json => if (json.isNull) Some(ConstNull) else None,
    "Expected null"
  )

  private def constInt = constValDecoder(
    json => json.number.filter(_.toBigDecimal.scale == 0).flatMap(_.toLong).map(ConstInt),
    "Expected int"
  )

  private def constDecimal = constValDecoder(
    json => json.number.map(n => ConstDecimal(n.toBigDecimal)),
    "Expected double"
  )

  private def constString = constValDecoder(
    json => json.string.map(ConstString),
    "Expected string"
  )

  private def typeCast = tagged[Expr]("TYPECAST", DecodeJson.derive[TypeCast])

  private def columnRef = tagged[Expr]("COLUMNREF", DecodeJson.derive[ColumnRef])

  private def aExpr = tagged[Expr]("AEXPR", DecodeJson.derive[AExpr])

  private def aExprAnd = tagged[Expr]("AEXPR AND", DecodeJson.derive[AExprAnd])

  private def aExprOr = tagged[Expr]("AEXPR OR", DecodeJson.derive[AExprOr])

  private def aExprNot = tagged[Expr]("AEXPR NOT", DecodeJson.derive[AExprNot])

  private def coalesce = tagged[Expr]("COALESCE", DecodeJson.derive[Coalesce])

  private def funcCall = tagged[Expr]("FUNCCALL", DecodeJson.derive[FuncCall])

  private def paramRef = tagged[Expr]("PARAMREF", DecodeJson.derive[ParamRef])
}

sealed case class TypeName(
  names: List[String],
  arrayBounds: Option[List[Int]] = None,
  pct_type: Boolean = false,
  setof: Boolean = false
)
object TypeName {
  implicit def typeNameDecodeJson: DecodeJson[TypeName] = DecodeJson(c =>
    tagged("TYPENAME", c, DecodeJson.derive[TypeName])
  )
}

sealed trait RangeRef

sealed case class RangeVar(
  relname: String,
  schemaname: Option[String] = None,
  alias: Option[Alias] = None
) extends RangeRef

sealed case class RangeSubSelect(
  subquery: SQL,
  alias: Option[Alias] = None,
  lateral: Boolean = false
) extends RangeRef

sealed case class JoinExpr(
  jointype: JoinType,
  larg: RangeRef,
  rarg: RangeRef,
  usingClause: Option[List[String]] = None,
  quals: Option[Expr] = None,
  alias: Option[Alias] = None,
  isNatural: Boolean = false
) extends RangeRef

object RangeRef {
  implicit def rangeRefDecodeJson: DecodeJson[RangeRef] =
    rangeVar ||| rangeSubSelect ||| joinExpr

  private def rangeVar = tagged[RangeRef]("RANGEVAR", DecodeJson.derive[RangeVar])

  private def rangeSubSelect = tagged[RangeRef]("RANGESUBSELECT", DecodeJson.derive[RangeSubSelect])

  private def joinExpr = tagged[RangeRef]("JOINEXPR", DecodeJson.derive[JoinExpr])
}

sealed abstract class JoinType(val toEnum: Int)
object JoinType {
  case object Inner extends JoinType(0)
  case object Left extends JoinType(1)
  case object Full extends JoinType(2)
  case object Right extends JoinType(3)

  def fromEnum(n: Int): Option[JoinType] = {
    List(Inner, Left, Full, Right).collectFirst {
      case x if x.toEnum == n => x
    }
  }

  implicit def joinTypeDecodeJson: DecodeJson[JoinType] = optionDecoder(
    json => json.number.flatMap(_.toInt).flatMap(fromEnum),
    "Invalid value for JoinType"
  )
}

/** Alias for range variable, might also list column aliases. */
sealed case class Alias(
  aliasname: String,
  colnames: Option[List[String]] = None
)
object Alias {
  implicit def aliasDecodeJson: DecodeJson[Alias] = DecodeJson(c =>
    tagged("ALIAS", c, DecodeJson.derive[Alias])
  )
}

sealed case class SortBy(
  node: Expr,
  sortby_dir: SortByDir = SortByDir.Default,
  sortby_nulls: SortByNulls = SortByNulls.Default,
  useOp: Option[List[String]] = None
)
object SortBy {
  implicit def sortByDecodeJson: DecodeJson[SortBy] = DecodeJson(c =>
    tagged("SORTBY", c, DecodeJson.derive[SortBy])
  )
}

sealed abstract class SortByDir(val toEnum: Int)
object SortByDir {
  case object Default extends SortByDir(0)
  case object Asc extends SortByDir(1)
  case object Desc extends SortByDir(2)
  case object Using extends SortByDir(3)

  def fromEnum(n: Int): Option[SortByDir] = {
    List(Default, Asc, Desc, Using).collectFirst {
      case x if x.toEnum == n => x
    }
  }

  implicit def sortByDirDecodeJson: DecodeJson[SortByDir] = optionDecoder(
    json => json.number.flatMap(_.toInt).flatMap(fromEnum),
    "Invalid value for SortByDir"
  )
}

sealed abstract class SortByNulls(val toEnum: Int)
object SortByNulls {
  case object Default extends SortByNulls(0)
  case object First extends SortByNulls(1)
  case object Last extends SortByNulls(2)

  def fromEnum(n: Int): Option[SortByNulls] = {
    List(Default, First, Last).collectFirst {
      case x if x.toEnum == n => x
    }
  }

  implicit def sortByNullsDecodeJson: DecodeJson[SortByNulls] = optionDecoder(
    json => json.number.flatMap(_.toInt).flatMap(fromEnum),
    "Invalid value for SortByNulls"
  )
}

sealed case class ResTarget(`val`: Expr, name: Option[String] = None)
object ResTarget {
  implicit def resTargetDecodeJson: DecodeJson[ResTarget] = DecodeJson(c =>
    tagged("RESTARGET", c, DecodeJson.derive[ResTarget])
  )
}

sealed case class WindowClause(
  name: String,
  partitionClause: Option[List[Expr]] = None,
  orderClause: Option[List[SortBy]] = None
)
object WindowClause {
  implicit def windowClauseDecodeJson: DecodeJson[WindowClause] = DecodeJson(c =>
    tagged("WINDOWDEF", c, DecodeJson.derive[WindowClause])
  )
}

sealed case class WithClause(
  ctes: List[CommonTableExpr],
  recursive: Boolean
)
object WithClause {
  implicit def withClauseDecodeJson: DecodeJson[WithClause] = DecodeJson(c =>
    tagged("WITHCLAUSE", c, DecodeJson.derive[WithClause])
  )
}

sealed case class CommonTableExpr(
  ctename: String,
  ctequery: SQL,
  aliascolnames: Option[List[String]] = None,
  ctecolnames: Option[List[String]] = None,
  cterecursive: Boolean = false,
  cterefcount: Int = 0
  // cteColCollations,
  // cteColTypes,
  // cteColTypMods,
)
object CommonTableExpr {
  implicit def commonTableExprDecodeJson: DecodeJson[CommonTableExpr] = DecodeJson(c =>
    tagged("COMMONTABLEEXPR", c, DecodeJson.derive[CommonTableExpr])
  )
}

object Types {
  val BOOL = TypeName(List("pg_catalog", "bool"))
}

object Consts {
  val TRUE = TypeCast(ConstString("t"), Types.BOOL)
  val FALSE = TypeCast(ConstString("f"), Types.BOOL)
}
