package squid.meta

import java.sql.ResultSet

import scala.annotation.StaticAnnotation

/** Default type registry used by macros. */
object TypeRegistry extends BaseTypeRegistry

/** Annotation used to register a method in an instance of BaseTypeRegistry. */
case class RegisterType(namespace: String, typeName: String) extends StaticAnnotation

/**
  * Provides a base registry mapping of PostgreSQL types to methods which are used to
  * retrieve values from a JDBC ResultSet.
  *
  * You can provide your own type registry by defining an object which defines methods
  * in a similar fashion.  You do not have to extend from BaseTypeRegistry, but chances
  * are you'll want to to avoid having to reimplement those predefined methods yourself.
  */
trait BaseTypeRegistry {
  @RegisterType("pg_catalog", "int4")
  def toInt4(rs: ResultSet, index: Int): Long = rs.getLong(index)

  @RegisterType("pg_catalog", "text")
  def toText(rs: ResultSet, index: Int): String = rs.getString(index)
}
