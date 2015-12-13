package squid.macros

import java.sql.Connection

import org.specs2.matcher.MatchResult

import squid.meta.DBUtils
import squid.test.DBSpecBase

/**
  * Tests for Query macros
  */
class QueryMacroSpec extends DBSpecBase { def is = s2"""
    MacroSpec
      simple query list $simpleQueryList
      query generates field names $queryGeneratesFieldNames
      query with params list $queryWithParamsList
  """

  def simpleQueryList = withInitialData { implicit c =>
    Bars.fetch().toList === List(
      Bars.Row(1, Some("a")),
      Bars.Row(2, None)
    )
  }

  def queryGeneratesFieldNames = withInitialData { implicit c =>
    val row = Bars.fetch().head
    sequence(
      row.id === 1,
      row.quux === Some("a")
    )
  }

  def queryWithParamsList = withInitialData { implicit c =>
    sequence(
      BarsByQuux(Some("a")).fetch().toList === List(BarsByQuux.Row(1, Some("a"))),
      BarsByQuux(None).fetch().toList === List(BarsByQuux.Row(2, None)),
      BarsByQuux(Some("foo")).fetch().toList === Nil
    )
  }

  private def withInitialData[A](block: Connection => A): A = {
    withRollback { implicit c =>
      DBUtils.execute("insert into foo.bar values (1, 'a'), (2, null)")
      block(c)
    }
  }
}

@Query object Bars {"""
  select id, quux
  from foo.bar
  order by id
"""}

// TODO: Type checker should error on `quux = ?` when `?` is Option.
// Should suggest `quux is not distinct from ?` since `quux = null` will always be false.
@Query class BarsByQuux(quux: Option[String]) {s"""
  select id, quux
  from foo.bar
  where quux is not distinct from $quux
  order by id
"""}
