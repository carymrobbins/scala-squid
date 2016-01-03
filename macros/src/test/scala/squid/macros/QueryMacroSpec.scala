package squid.macros

import java.sql.Connection

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

  def queryObjFetch = withRollback { implicit c =>
    Bars.fetch().toList === List(
      Bars.Row(1, Some("alpha")),
      Bars.Row(2, None),
      Bars.Row(3, Some("charlie"))
    )
  }

  def queryObjFields = withRollback { implicit c =>
    val row = Bars.fetch().head
    sequence(
      row must beAnInstanceOf[Bars.Row],
      row.id === 1,
      row.quux === Some("alpha")
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

  def queryClassParams = withRollback { implicit c =>
    sequence(
      BarsByQuux(Some("alpha")).fetch().toList === List(BarsByQuux.Row(1, Some("alpha"))),
      BarsByQuux(None).fetch().toList === List(BarsByQuux.Row(2, None)),
      BarsByQuux(Some("gamma")).fetch().toList === Nil
    )
  }

  def queryClassFields = withRollback { implicit c =>
    val row = BarsByQuux(None).fetch().head
    sequence(
      row must beAnInstanceOf[BarsByQuux.Row],
      row.id === 2,
      row.quux === None
    )
  }
}
