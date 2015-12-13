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
