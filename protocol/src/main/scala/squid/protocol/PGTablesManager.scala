package squid.protocol

import scala.collection.mutable

import squid.util.StreamAssertions

/** Manages fetching and caching PostgreSQL table names/OIDs. */
final class PGTablesManager(query: PGQueryExecutor) {

  /** Gets the system OID for the provided table. */
  def getOID(schema: String, table: String): Option[OID] = {
    val tableRef = TableRef(schema, table)
    cache.get(tableRef) match {
      case Some(oid) => Some(oid)

      case None =>
        val result = query.prepared(
          """
            select pg_class.oid
            from pg_catalog.pg_class, pg_catalog.pg_namespace
            where pg_class.relnamespace = pg_namespace.oid and
                  pg_namespace.nspname = $1 and
                  pg_class.relname = $2
          """,
          List(PGParam.from(schema), PGParam.from(table))
        ).flatten
        StreamAssertions.zeroOrOne(result).map { v =>
          val oid = v.as[OID]
          cache.update(tableRef, oid)
          oid
        }
    }
  }

  case class TableRef(schema: String, table: String)

  private val cache = mutable.Map.empty[TableRef, OID]
}
