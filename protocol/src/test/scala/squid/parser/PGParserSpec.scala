package squid.parser

import org.specs2._
import org.specs2.matcher.MatchResult
import squid.protocol.{PGConnection, OID, PGConnectInfo, PGProtocol}

/** Tests for consuming parse trees from PostgreSQL. */
class PGParserSpec extends Specification { def is = s2"""
  PGParser should
    parse selectSimple $selectSimple
  """
//    parse selectSimple $selectSimple
//    parse selectLiterals $selectLiterals
//    parse selectExprFromTable $selectExprFromTable
//    parse selectFromJoin $selectFromJoin
//    parse selectWhereParam $selectWhereParam
//    parse selectAndOrNot $selectAndOrNot
//    parse selectCoalesce $selectCoalesce
//    parse isDistinctFrom $isDistinctFrom
//  """

  def selectSimple = PGProtocol.withConnection(INFO) { c =>
    val q = parse(c, """
      select id, quux from foo.bar
    """)
    sequence(
      q.commandType === CmdType.Select,
      q.querySource === QuerySource.Original,
      q.canSetTag === true,
      q.utilityStmt === None,
      q.resultRelation === 0,
      q.hasAggs === false,
      q.hasWindowFuncs === false,
      q.hasSubLinks === false,
      q.hasDistinctOn === false,
      q.hasRecursive === false,
      q.hasModifyingCTE === false,
      q.hasForUpdate === false
    )
  }

//  def selectLiterals = PGProtocol.withConnection(INFO) { c =>
//    parse(c, """
//      select 1, 2.0 as b, 'foo' as c, true as d, null as e
//    """).isSuccessful(
//      Select(
//        List(
//          ResTarget(ConstInt(1)),
//          ResTarget(ConstDecimal(2.0), Some("b")),
//          ResTarget(ConstString("foo"), Some("c")),
//          ResTarget(Consts.TRUE, Some("d")),
//          ResTarget(ConstNull, Some("e"))
//        )
//      )
//    )
//  }

//  def selectExprFromTable = {
//    parse("""
//      select a + b as c, f(d), *
//      from foo.bar as baz(a, b, d)
//    """).isSuccessful(
//      Select(
//        List(
//          ResTarget(
//            AExpr(
//              ColumnRef.named("a"),
//              List("+"),
//              ColumnRef.named("b")
//            ),
//            Some("c")
//          ),
//          ResTarget(
//            FuncCall(List("f"), Some(List(ColumnRef.named("d"))))
//          ),
//          ResTarget(
//            ColumnRef(List(FieldPart.Star))
//          )
//        ),
//        fromClause = Some(List(
//          RangeVar(
//            relname = "bar",
//            schemaname = Some("foo"),
//            alias = Some(Alias("baz", Some(List("a", "b", "d"))))
//          )
//        ))
//      )
//    )
//  }
//
//  def selectFromJoin = {
//    parse("""
//      select a
//      from b
//      join c using (d)
//      left join e on b.a = e.g
//      natural right join x
//    """).isSuccessful(
//      Select(
//        List(ResTarget(ColumnRef.named("a"))),
//        fromClause = Some(List(
//          JoinExpr(
//            JoinType.Right,
//            JoinExpr(
//              JoinType.Left,
//              JoinExpr(
//                JoinType.Inner,
//                RangeVar("b"),
//                RangeVar("c"),
//                usingClause = Some(List("d"))
//              ),
//              RangeVar("e"),
//              quals = Option(
//                AExpr(
//                  ColumnRef.named("b", "a"),
//                  List("="),
//                  ColumnRef.named("e", "g")
//                )
//              )
//            ),
//            RangeVar("x"),
//            isNatural = true
//          )
//        ))
//      )
//    )
//  }
//
//  def selectWhereParam = {
//    parse("""
//      select a
//      from b
//      where c between $1 and $2
//    """).isSuccessful(
//      Select(
//        List(ResTarget(ColumnRef.named("a"))),
//        fromClause = Some(List(RangeVar("b"))),
//        whereClause = Some(
//          AExprAnd(
//            AExpr(
//              ColumnRef.named("c"),
//              List(">="),
//              ParamRef(1)
//            ),
//            AExpr(
//              ColumnRef.named("c"),
//              List("<="),
//              ParamRef(2)
//            )
//          )
//        )
//      )
//    )
//  }
//
//  def selectAndOrNot = {
//    parse("""
//      select not a and b or c
//    """).isSuccessful(
//      Select(
//        List(ResTarget(
//          AExprOr(
//            AExprAnd(
//              AExprNot(ColumnRef.named("a")),
//              ColumnRef.named("b")
//            ),
//            ColumnRef.named("c")
//          )
//        ))
//      )
//    )
//  }
//
//  def selectCoalesce = {
//    parse("""
//      select coalesce(a, b, c, 1)
//    """).isSuccessful(
//      Select(
//        List(ResTarget(Coalesce(List(
//          ColumnRef.named("a"),
//          ColumnRef.named("b"),
//          ColumnRef.named("c"),
//          ConstInt(1)
//        ))))
//      )
//    )
//  }
//
//  def isDistinctFrom = {
//    parse("""
//      select a
//      from b
//      where c is distinct from d
//    """).isSuccessful(
//      Select(
//        List(ResTarget(ColumnRef.named("a"))),
//        fromClause = Some(List(RangeVar("b"))),
//        whereClause = Some(AExprDistinct(ColumnRef.named("c"), ColumnRef.named("d")))
//      )
//    )
//  }

  def sequence[T](ms: MatchResult[T]*): MatchResult[Seq[T]] = MatchResult.sequence(ms)

  private def parse(c: PGConnection, sql: String, types: List[OID] = Nil): Query = {
    Query.fromExpr(PGParser.parse(c.describe(sql, types).parseTree).get).fold(
      e => throw new RuntimeException(e),
      identity
    )
  }

  private val INFO = PGConnectInfo(
    host = "localhost",
    port = 5432,
    timeout = 5,
    user = "squid",
    password = "squid",
    database = "squid",
    debug = false
  )
}
