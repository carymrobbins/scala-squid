package squid.protocol

import scala.collection.mutable

/** Manages fetching and caching PostgreSQL table names/OIDs. */
final class PGTablesManager(query: PGQueryExecutor) {

  /** Gets the system OID for the provided table. */
  def getOID(schema: String, table: String): Option[OID] = {
    val tableRef = TableRef(schema, table)
    cache.get(tableRef) match {
      case Some(oid) => Some(oid)

      case None =>
        query.prepared(
          """
            select pg_class.oid
            from pg_catalog.pg_class, pg_catalog.pg_namespace
            where pg_class.relnamespace = pg_namespace.oid and
                  pg_namespace.nspname = $1 and
                  pg_class.relname = $2
          """,
          List(PGParam.from(schema), PGParam.from(table))
        ).flatten match {
          case List(v) =>
            val oid = v.as[OID]
            cache.update(tableRef, oid)
            Some(oid)

          case Nil => None
          case other => throw new RuntimeException(s"Expected zero or one element, got: $other")
        }
    }
  }

  case class TableRef(schema: String, table: String)

  private val cache = mutable.Map.empty[TableRef, OID]
}
