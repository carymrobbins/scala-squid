package squid.meta

import java.sql.Connection

import scala.collection.mutable

/**
  * Holds meta data associations for tables and their columns.
  * WARNING: The underlying data structure is mutable.
  */
final class TableMetaData(
  val underlying: scala.collection.mutable.Map[TableMetaData.Key, List[Meta.Column]]
) extends AnyVal {

  def get(schema: Option[String], table: String): Option[List[Meta.Column]] = {
    underlying.get(TableMetaData.Key(schema, table))
  }

  def set(schema: Option[String], table: String, columns: List[Meta.Column]): Unit = {
    underlying(TableMetaData.Key(schema, table)) = columns
  }
}

object TableMetaData {
  case class Key(schema: Option[String], table: String)

  def init()(implicit c: Connection): TableMetaData = {
    val tmd = new TableMetaData(mutable.Map())
    Meta.getTables(None, None).foreach { table =>
      val columns = Meta.getColumns(table.catalog, table.schema, table.name)
      tmd.set(table.schema, table.name, columns)
    }
    tmd
  }
}
