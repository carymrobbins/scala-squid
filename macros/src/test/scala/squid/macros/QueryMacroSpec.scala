package squid.macros

import java.sql.Connection

import org.specs2.matcher.MatchResult

import squid.meta.DBUtils
import squid.test.DBSpecBase

/**
  * Tests for Query macros
  */
class QueryMacroSpec extends DBSpecBase { def is = s2"""
    @Query
      object .fetch() $queryObjFetch
      object generates row with fields $queryObjFields
      class with params .fetch() $queryClassParams
      class with params generates row with fields $queryClassFields
  """

  @Query object Bars {"""
    select id, quux
    from foo.bar
    order by id
  """}

  def queryObjFetch = withInitialData { implicit c =>
    Bars.fetch().toList === List(
      Bars.Row(1, Some("a")),
      Bars.Row(2, None)
    )
  }

  def queryObjFields = withInitialData { implicit c =>
    val row = Bars.fetch().head
    sequence(
      row must beAnInstanceOf[Bars.Row],
      row.id === 1,
      row.quux === Some("a")
    )
  }

  // TODO: Type checker should error on `quux = ?` when `?` is Option.
  // Should suggest `quux is not distinct from ?` since `quux = null` will always be false.
  @Query class BarsByQuux(quux: Option[String]) {s"""
    select id, quux
    from foo.bar
    where quux is not distinct from $quux
    order by id
  """}

  def queryClassParams = withInitialData { implicit c =>
    sequence(
      BarsByQuux(Some("a")).fetch().toList === List(BarsByQuux.Row(1, Some("a"))),
      BarsByQuux(None).fetch().toList === List(BarsByQuux.Row(2, None)),
      BarsByQuux(Some("foo")).fetch().toList === Nil
    )
  }

  def queryClassFields = withInitialData { implicit c =>
    val row = BarsByQuux(None).fetch().head
    sequence(
      row must beAnInstanceOf[BarsByQuux.Row],
      row.id === 2,
      row.quux === None
    )
  }

  /** Populate DB for each test. */
  private def withInitialData[A](block: Connection => A): A = {
    withRollback { implicit c =>
      DBUtils.execute("insert into foo.bar values (1, 'a'), (2, null)")
      block(c)
    }
  }
}
