package squid.protocol

import squid.util.{StreamAssertions, BiMap}

/** Manages fetching and caching PostgreSQL type names/OIDs. */
final class PGTypesManager(query: PGQueryExecutor) {

  /**
    * Gets the system OID for the respective Scala type, given the Scala type has an instance
    * of the PGType typeclass.
    */
  //noinspection AccessorLikeMethodIsEmptyParen
  def getOID[A : PGType](): OID = {
    val (namespace, typeName) = PGType.tupled[A]
    getOID(namespace, typeName).getOrElse {
      throw new RuntimeException(s"No oid found for type: $namespace.$typeName")
    }
  }

  /** Gets the system OID for a type given its qualified name. */
  def getOID(namespace: String, typeName: String): Option[OID] = {
    val pgTypeName = PGTypeName(namespace, typeName)
    cache.getByLeft(pgTypeName).orElse {
      val result = query.prepared(
        """
          select pg_type.oid
          from pg_catalog.pg_type, pg_catalog.pg_namespace
          where pg_type.typnamespace = pg_namespace.oid and
                pg_namespace.nspname = $1 and
                pg_type.typname = $2
        """,
        List(PGParam.from(namespace), PGParam.from(typeName))
      ).flatten
      StreamAssertions.zeroOrOne(result).map { v =>
        val oid = v.as[OID]
        cache.update(pgTypeName, oid)
        oid
      }
    }
  }

  /** Gets the name of a type given its OID. */
  def getName(oid: OID): Option[PGTypeName] = {
    cache.getByRight(oid).orElse {
      val results = query.prepared(
        """
          select pg_namespace.nspname, pg_type.typname
          from pg_catalog.pg_type, pg_catalog.pg_namespace
          where pg_type.typnamespace = pg_namespace.oid and
                pg_type.oid = $1
        """,
        List(PGParam.from(oid))
      ).flatten
      StreamAssertions.zeroOrTwo(results).map { case (namespace, typeName) =>
        val result = PGTypeName(namespace.as[String], typeName.as[String])
        cache.update(result, oid)
        result
      }
    }
  }

  private val cache = BiMap.empty[PGTypeName, OID]
}
