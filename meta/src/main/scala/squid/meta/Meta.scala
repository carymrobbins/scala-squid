package squid.meta

import java.sql.{Types, Connection, DatabaseMetaData, ResultSet}

import scala.annotation.StaticAnnotation
import scala.collection.mutable

import DBImplicits._

/**
  * TODO
  */
object Meta {
  def getTables
      (catalog: Option[String], schemaPattern: Option[String])
      (implicit c: Connection): List[Table] = {
    c.getMetaData.getTables(catalog.orNull, schemaPattern.orNull, null, null).map(
      Table.unsafeFromResultSet
    ).toList
  }

  def getColumns
      (catalog: Option[String], schemaPattern: Option[String], tablePattern: String)
      (implicit c: Connection)
      : List[Column] = {
    c.getMetaData.getColumns(catalog.orNull, schemaPattern.orNull, tablePattern, null).map(
      Column.unsafeFromResultSet
    ).toList
  }

  /** Gets class name for SQL type. */
  // TODO: There's got to be a better way.
  // Couldn't seem to figure out how to return Class[_] and turn it into
  // TypeName in the macro.  There's `.getName`, but that doesn't exactly
  // work in a lot of cases.
  def getClassFromColumn(column: Column): String = {
    // TODO: May rather match on column.typeName
    column.javaType match {
      case Types.INTEGER => "Long"
      case Types.VARCHAR => "String"
      case other => throw new RuntimeException(s"Unsupported java type '$other'")
    }
  }

  case class Table(
    catalog: Option[String],
    schema: Option[String],
    name: String,
    tableType: String
  )

  object Table {
    def unsafeFromResultSet(rs: ResultSet): Table = Table(
      Option(rs.getString("TABLE_CAT")),
      Option(rs.getString("TABLE_SCHEM")),
      rs.getString("TABLE_NAME"),
      rs.getString("TABLE_TYPE")
    )
  }

  case class Column(
    name: String,
    javaType: Int,
    typeName: String,
    nullable: Boolean
  )

  object Column {
    def unsafeFromResultSet(rs: ResultSet): Column = Column(
      rs.getString("COLUMN_NAME"),
      rs.getInt("DATA_TYPE"),
      rs.getString("TYPE_NAME"),
      rs.getShort("NULLABLE") match {
        case DatabaseMetaData.columnNoNulls => false
        case DatabaseMetaData.columnNullable => true
        case DatabaseMetaData.columnNullableUnknown =>
          // TODO: Is it better to assume nullable here?
          true
        case other =>
          throw new RuntimeException(s"Unexpected NULLABLE value in meta data: $other")
      }
    )
  }
}

/**
  * Holds meta data associations for tables and their columns.
  * WARNING: The underlying data structure is mutable.
  */
final class TableMetaData(
  val underlying: mutable.Map[TableMetaData.Key, List[Meta.Column]]
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
