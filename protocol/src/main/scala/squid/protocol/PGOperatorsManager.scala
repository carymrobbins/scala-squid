package squid.protocol

import scala.collection.mutable

/** Manages fetching and caching PostgreSQL operator names/OIDs. */
final class PGOperatorsManager(query: PGQueryExecutor) {
  /** Gets the qualified operator name for the given OID. */
  def getName(oid: OID): Option[PGOperatorName] = {
    cache.get(oid).orElse {
      query.prepared(
        """
          select pg_namespace.nspname, pg_operator.oprname
          from pg_catalog.pg_operator, pg_catalog.pg_namespace
          where pg_operator.oprnamespace = pg_namespace.oid and
                pg_operator.oid = $1
        """,
        List(PGParam.from(oid))
      ).flatten match {
        case Nil => None

        case List(namespace, name) =>
          val op = PGOperatorName(namespace.as[String], name.as[String])
          cache.update(oid, op)
          Some(op)

        case other => throw new RuntimeException(s"Expected zero or two elements, got: $other")
      }
    }
  }

  private val cache = mutable.Map.empty[OID, PGOperatorName]
}

final case class PGOperatorName(namespace: String, name: String)
