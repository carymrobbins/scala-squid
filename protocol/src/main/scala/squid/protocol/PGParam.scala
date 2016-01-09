package squid.protocol

/** A parameter to be passed via a prepared query. */
final case class PGParam(value: PGValue, typeOID: Option[OID] = None) {
  def typed(o: OID): PGParam = copy(typeOID = Some(o))
}

object PGParam {
  /** Builds a PGParam from a Scala value given it has a ToPGValue instance. */
  def from[A : ToPGValue](a: A): PGParam = PGParam(ToPGValue.from(a), None)
}
