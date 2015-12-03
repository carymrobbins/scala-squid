package squid.parser

import org.specs2._
import squid.parser.ast._

/** Tests for consuming parse trees for PostgreSQL. */
class PGParserSpec extends Specification { def is = s2"""
  PGParser should
    parse select literals $selectLiterals
    parse select expr from table $selectExprFromTable
    parse select from join $selectFromJoin
    parse select where param $selectWhereParam
    parse select and or not $selectAndOrNot
    parse select coalesce $selectCoalesce
  """

  def selectLiterals = {
    PGParser.parse("""
      select 1, 2.0 as b, 'foo' as c, true as d, null as e
    """).isSuccessful(
      Select(
        List(
          ResTarget(ConstInt(1)),
          ResTarget(ConstDecimal(2.0), Some("b")),
          ResTarget(ConstString("foo"), Some("c")),
          ResTarget(Consts.TRUE, Some("d")),
          ResTarget(ConstNull, Some("e"))
        )
      )
    )
  }

  def selectExprFromTable = {
    PGParser.parse("""
      select a + b as c, f(d), *
      from foo.bar as baz(a, b, d)
    """).isSuccessful(
      Select(
        List(
          ResTarget(
            AExpr(
              ColumnRef.named("a"),
              List("+"),
              ColumnRef.named("b")
            ),
            Some("c")
          ),
          ResTarget(
            FuncCall(List("f"), Some(List(ColumnRef.named("d"))))
          ),
          ResTarget(
            ColumnRef(List(FieldPart.Star))
          )
        ),
        fromClause = Some(List(
          RangeVar(
            relname = "bar",
            schemaname = Some("foo"),
            alias = Some(Alias("baz", Some(List("a", "b", "d"))))
          )
        ))
      )
    )
  }

  def selectFromJoin = {
    PGParser.parse("""
      select a
      from b
      join c using (d)
      left join e on b.a = e.g
      natural right join x
    """).isSuccessful(
      Select(
        List(ResTarget(ColumnRef.named("a"))),
        fromClause = Some(List(
          JoinExpr(
            JoinType.Right,
            JoinExpr(
              JoinType.Left,
              JoinExpr(
                JoinType.Inner,
                RangeVar("b"),
                RangeVar("c"),
                usingClause = Some(List("d"))
              ),
              RangeVar("e"),
              quals = Option(
                AExpr(
                  ColumnRef.named("b", "a"),
                  List("="),
                  ColumnRef.named("e", "g")
                )
              )
            ),
            RangeVar("x"),
            isNatural = true
          )
        ))
      )
    )
  }

  def selectWhereParam = {
    PGParser.parse("""
      select a
      from b
      where c between $1 and $2
    """).isSuccessful(
      Select(
        List(ResTarget(ColumnRef.named("a"))),
        fromClause = Some(List(RangeVar("b"))),
        whereClause = Some(
          AExprAnd(
            AExpr(
              ColumnRef.named("c"),
              List(">="),
              ParamRef(1)
            ),
            AExpr(
              ColumnRef.named("c"),
              List("<="),
              ParamRef(2)
            )
          )
        )
      )
    )
  }

  def selectAndOrNot = {
    PGParser.parse("""
      select not a and b or c
    """).isSuccessful(
      Select(
        List(ResTarget(
          AExprOr(
            AExprAnd(
              AExprNot(ColumnRef.named("a")),
              ColumnRef.named("b")
            ),
            ColumnRef.named("c")
          )
        ))
      )
    )
  }

  def selectCoalesce = {
    PGParser.parse("""
      select coalesce(a, b, c, 1)
    """).isSuccessful(
      Select(
        List(ResTarget(Coalesce(List(
          ColumnRef.named("a"),
          ColumnRef.named("b"),
          ColumnRef.named("c"),
          ConstInt(1)
        ))))
      )
    )
  }

  implicit final class RichParseResult[A](val underlying: PGParser.ParseResult) {
    def isSuccessful(expected: Any) = underlying.fold(
      e => failure.updateMessage(e),
      r => (r === expected).toResult
    )
  }
}
