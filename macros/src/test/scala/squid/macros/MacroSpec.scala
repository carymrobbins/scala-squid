package squid.macros

import squid.meta.DBUtils
import squid.test.DBSpecBase

/**
  * TODO
  */
class MacroSpec extends DBSpecBase { def is = s2"""
    MacroSpec
      simple query list $simpleQueryList
  """

  def simpleQueryList = withRollback { implicit c =>
    DBUtils.execute("insert into foo.bar values (1, 'a'), (2, null)")
    Bars.fetch().toList === List(
      Bars.Row(1, Some("a")),
      Bars.Row(2, None)
    )
  }
}

@Query object Bars {"""
  select id, quux
  from foo.bar
  order by id
"""}

//@Query class BarsByQuux(quux: Option[String]) {s"""
//  select id, quux
//  from foo.bar
//  where quux = $quux
//  order by id
//"""}
