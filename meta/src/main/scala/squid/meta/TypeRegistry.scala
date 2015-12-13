package squid.meta

import java.sql.ResultSet

import scala.annotation.StaticAnnotation

object TypeRegistry extends BaseTypeRegistry

case class RegisterType(typeName: String) extends StaticAnnotation

trait BaseTypeRegistry {
  @RegisterType("int4")
  def toInt4(rs: ResultSet, index: Int): Long = rs.getLong(index)

  @RegisterType("text")
  def toText(rs: ResultSet, index: Int): String = rs.getString(index)
}
