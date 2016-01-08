package squid.parser

import com.typesafe.config.ConfigFactory
import org.specs2._
import org.specs2.matcher.MatchResult
import squid.protocol.{PGConnection, OID, PGConnectInfo, PGProtocol}

/** Tests for consuming parse trees from PostgreSQL. */
class PGParserSpec extends Specification { def is = s2"""
  PGParser should
    parse selectSimple $selectSimple
    parse selectLiterals $selectLiterals
  """
//    parse selectExprFromTable $selectExprFromTable
//    parse selectFromJoin $selectFromJoin
//    parse selectWhereParam $selectWhereParam
//    parse selectAndOrNot $selectAndOrNot
//    parse selectCoalesce $selectCoalesce
//    parse isDistinctFrom $isDistinctFrom
//  """

  def selectSimple = PGProtocol.withConnection(INFO) { c =>
    parse(c, """
      select id, quux from foo.bar
    """) === defaultQuery.copy(
      rtable = Some(List(
        RangeTblEntry(
          alias = None,
          eref = Some(Alias(
            aliasname = "bar",
            colnames = Some(List("id", "quux"))
          )),
          rtekind = RTEKind.Relation,
          relid = tableOID(c, "foo", "bar"),
          relkind = RelKind.Table,
          lateral = false,
          inh = true,
          inFromCl = true
        )
      )),
      jointree = FromExpr(
        fromlist = Some(List(RangeTblRef(rtindex = 1))),
        quals = None
      ),
      targetList = List(
        TargetEntry(
          expr = Var(
            varno = 1,
            varattno = 1,
            vartype = c.getTypeOID("pg_catalog", "int4"),
            varlevelsup = 0
          ),
          resno = 1,
          resname = Some("id"),
          ressortgroupref = 0,
          resorigtbl = tableOID(c, "foo", "bar"),
          resorigcol = 1
        ),
        TargetEntry(
          expr = Var(
            varno = 1,
            varattno = 2,
            vartype = c.getTypeOID("pg_catalog", "text"),
            varlevelsup = 0
          ),
          resno = 2,
          resname = Some("quux"),
          ressortgroupref = 0,
          resorigtbl = tableOID(c, "foo", "bar"),
          resorigcol = 2
        )
      )
    )
  }

  def selectLiterals = PGProtocol.withConnection(INFO) { c =>
    parse(c, """
      select 1, 2.0 as b, 'foo' as c, true as d, null as e
    """, Nil) === defaultQuery.copy(
      targetList = List(
        TargetEntry(
          expr = Const(
            consttype = c.getTypeOID("pg_catalog", "int4"),
            constisnull = false
          ),
          resno = 1,
          resname = None,
          ressortgroupref = 0,
          resorigtbl = OID(0),
          resorigcol = 0
        ),
        TargetEntry(
          expr = Const(
            // text literals are typed as unknown
            consttype = c.getTypeOID("pg_catalog", "numeric"),
            constisnull = false
          ),
          resno = 2,
          resname = Some("b"),
          ressortgroupref = 0,
          resorigtbl = OID(0),
          resorigcol = 0
        ),
        TargetEntry(
          expr = Const(
            consttype = c.getTypeOID("pg_catalog", "unknown"),
            constisnull = false
          ),
          resno = 3,
          resname = Some("c"),
          ressortgroupref = 0,
          resorigtbl = OID(0),
          resorigcol = 0
        ),

        TargetEntry(
          expr = Const(
            consttype = c.getTypeOID("pg_catalog", "bool"),
            constisnull = false
          ),
          resno = 4,
          resname = Some("d"),
          ressortgroupref = 0,
          resorigtbl = OID(0),
          resorigcol = 0
        ),
        TargetEntry(
          expr = Const(
            consttype = c.getTypeOID("pg_catalog", "unknown"),
            constisnull = true
          ),
          resno = 5,
          resname = Some("e"),
          ressortgroupref = 0,
          resorigtbl = OID(0),
          resorigcol = 0
        )
      )
    )
  }

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
    val parseTree = c.describe(sql, types).parseTree
    val result = PGParser.parse(parseTree) match {
      case PGParser.Success(r, _) => r
      case e: PGParser.NoSuccess =>
        throw new RuntimeException(s"PGParser.Error: $e\n\nFull parse tree: $parseTree")
    }
    Query.decode(result).fold(
      e => throw new RuntimeException(s"Query.decode error: $e\n\nFull parse tree: $parseTree"),
      identity
    )
  }

  private def tableOID(c: PGConnection, schema: String, table: String): OID = {
    c.getTableOID(schema, table).getOrElse {
      throw new RuntimeException("No oid found for foo.bar")
    }
  }

  private val defaultQuery = Query(
    commandType = CmdType.Select,
    querySource = QuerySource.Original,
    resultRelation = 0,
    hasAggs = false,
    hasWindowFuncs = false,
    hasSubLinks = false,
    hasDistinctOn = false,
    hasRecursive = false,
    hasModifyingCTE = false,
    hasForUpdate = false,
    cteList = None,
    rtable = None,
    jointree = FromExpr(None, None),
    targetList = Nil,
    withCheckOptions = None,
    returningList = None,
    groupClause = None,
    havingQual = None,
    windowClause = None,
    distinctClause = None,
    sortClause = None,
    limitOffset = None,
    limitCount = None,
    rowMarks = None,
    setOperations = None,
    constraintDeps = None
  )

  private val config = ConfigFactory.load()

  private val INFO = PGConnectInfo.fromConfig(config).copy(
    debug = false,
    prettyPrintParseTrees = true
  )
}
