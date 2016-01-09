package squid.protocol

import squid.util.BinaryReader

/** Newtype wrapper for a PostgreSQL system OID. */
// TODO: Should probably have phantom type param.
final case class OID(toInt: Int) extends AnyVal

object OID {
  def decode(r: BinaryReader): OID = OID(r.readInt32())
}
