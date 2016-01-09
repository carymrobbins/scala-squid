package squid.parser

import com.typesafe.config.ConfigFactory
import org.specs2._
import org.specs2.matcher.MatchResult
import squid.protocol._

/** Tests for consuming parse trees from PostgreSQL. */
class PGParserSpec extends Specification { def is = s2"""
  PGParser should
    parse selectSimple $selectSimple
    parse selectLiterals $selectLiterals
    parse selectWherePK $selectWherePK
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
        defaultRangeTblEntry.copy(
          eref = Some(Alias(aliasname = "bar", colnames = Some(List("id", "quux")))),
          relid = c.tables.getOID("foo", "bar").get
        )
      )),
      jointree = FromExpr(
        fromlist = Some(List(RangeTblRef(rtindex = 1))),
        quals = None
      ),
      targetList = Some(List(
        TargetEntry(
          expr = Var(
            varno = 1,
            varattno = 1,
            vartype = c.types.getOID("pg_catalog", "int4").get,
            varlevelsup = 0
          ),
          resno = 1,
          resname = Some("id"),
          ressortgroupref = 0,
          resorigtbl = c.tables.getOID("foo", "bar").get,
          resorigcol = 1
        ),
        TargetEntry(
          expr = Var(
            varno = 1,
            varattno = 2,
            vartype = c.types.getOID("pg_catalog", "text").get,
            varlevelsup = 0
          ),
          resno = 2,
          resname = Some("quux"),
          ressortgroupref = 0,
          resorigtbl = c.tables.getOID("foo", "bar").get,
          resorigcol = 2
        )
      ))
    )
  }

  def selectLiterals = PGProtocol.withConnection(INFO) { c =>
    parse(c, """
      select 1, 2.0 as b, 'foo' as c, true as d, null as e
    """, Nil) === defaultQuery.copy(
      targetList = Some(List(
        TargetEntry(
          expr = Const(
            consttype = c.types.getOID("pg_catalog", "int4").get,
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
            consttype = c.types.getOID("pg_catalog", "numeric").get,
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
            consttype = c.types.getOID("pg_catalog", "unknown").get,
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
            consttype = c.types.getOID("pg_catalog", "bool").get,
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
            consttype = c.types.getOID("pg_catalog", "unknown").get,
            constisnull = true
          ),
          resno = 5,
          resname = Some("e"),
          ressortgroupref = 0,
          resorigtbl = OID(0),
          resorigcol = 0
        )
      ))
    )
  }

  def selectWherePK = PGProtocol.withConnection(INFO) { c =>
    val result = parse(c, """
      select from foo.bar where id = $1
    """, Nil)

    val whereExpr = result.jointree.quals.get.asInstanceOf[OpExpr]

    sequence(
      c.operators.getName(whereExpr.opno) === Some(PGOperatorName("pg_catalog", "=")),

      whereExpr.args._1 === Var(
        varno = 1,
        varattno = 1,
        vartype = c.types.getOID("pg_catalog", "int4").get,
        varlevelsup = 0
      ),

      whereExpr.args._2 === Param(
        paramkind = ParamKind.Extern,
        paramid = 1,
        paramtype = c.types.getOID("pg_catalog", "int4").get
      )
    )
  }

//  def selectExprFromTable = {
//    parse("""
//      select a + b as c, f(d), *
//      from foo.bar as baz(a, b, d)
//    """)
//  }
//
//  def selectFromJoin = {
//    parse("""
//      select a
//      from b
//      join c using (d)
//      left join e on b.a = e.g
//      natural right join x
//    """)
//  }
//
//  def selectWhereParam = {
//    parse("""
//      select a
//      from b
//      where c between $1 and $2
//    """)
//  }
//
//  def selectAndOrNot = {
//    parse("""
//      select not a and b or c
//    """)
//  }
//
//  def selectCoalesce = {
//    parse("""
//      select coalesce(a, b, c, 1)
//    """)
//    )
//  }
//
//  def isDistinctFrom = {
//    parse("""
//      select a
//      from b
//      where c is distinct from d
//    """)
//  }

  def sequence[T](ms: MatchResult[T]*): MatchResult[Seq[T]] = MatchResult.sequence(ms)

  private def parse(c: PGConnection, sql: String, types: List[OID] = Nil): Query = {
    val parseTree = c.query.describe(sql, types).getOrThrow.parseTree
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
    targetList = None,
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

  private val defaultRangeTblEntry =  RangeTblEntry(
    alias = None,
    eref = None,
    rtekind = RTEKind.Relation,
    relid = OID(0),
    relkind = RelKind.Table,
    lateral = false,
    inh = true,
    inFromCl = true
  )

  private val config = ConfigFactory.load()

  private val INFO = PGConnectInfo.fromConfig(config).copy(
    debug = false,
    prettyPrintParseTrees = true
  )
}
