package squid.protocol

/**
  * A qualified PostgreSQL type name.
  * Also used as a typeclass to retrieve the PostgreSQL type name for Scala types.
  */
final case class PGType[A](underlying: PGTypeName) {
  def namespace: String = underlying.namespace
  def name: String = underlying.name
  def tupled: (String, String) = (namespace, name)
}

object PGType {
  def apply[A](namespace: String, typeName: String): PGType[A] = {
    PGType(PGTypeName(namespace, typeName))
  }

  def tupled[A : PGType]: (String, String) = implicitly[PGType[A]].tupled

  implicit val pgTypeOID: PGType[OID] = PGType("pg_catalog", "oid")

  implicit val pgTypeInt: PGType[Int] = PGType("pg_catalog", "int4")

  implicit val pgTypeString: PGType[String] = PGType("pg_catalog", "text")
}

final case class PGTypeName(namespace: String, name: String)
