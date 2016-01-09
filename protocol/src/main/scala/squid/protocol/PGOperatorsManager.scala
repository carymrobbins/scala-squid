package squid.protocol

import scala.collection.mutable

import squid.util.StreamAssertions

/** Manages fetching and caching PostgreSQL operator names/OIDs. */
final class PGOperatorsManager(query: PGQueryExecutor) {
  /** Gets the qualified operator name for the given OID. */
  def getName(oid: OID): Option[PGOperatorName] = {
    cache.get(oid).orElse {
      val results = query.prepared(
        """
          select pg_namespace.nspname, pg_operator.oprname
          from pg_catalog.pg_operator, pg_catalog.pg_namespace
          where pg_operator.oprnamespace = pg_namespace.oid and
                pg_operator.oid = $1
        """,
        List(PGParam.from(oid))
      ).flatten
      StreamAssertions.zeroOrTwo(results).map { case (namespace, name) =>
        val op = PGOperatorName(namespace.as[String], name.as[String])
        cache.update(oid, op)
        op
      }
    }
  }

  private val cache = mutable.Map.empty[OID, PGOperatorName]
}

final case class PGOperatorName(namespace: String, name: String)
