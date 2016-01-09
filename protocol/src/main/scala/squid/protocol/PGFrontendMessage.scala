package squid.protocol

import squid.util.BinaryWriter

/** Various messages sent from the frontend to the backend. */
sealed trait PGFrontendMessage {
  import PGFrontendMessage._

  /** Encodes the frontend message to binary. */
  def encode: Encoded = {
    val w = new BinaryWriter(PGProtocol.CHARSET)
    this match {
      case msg: StartupMessage =>
        w.writeBytes(PGProtocol.PROTOCOL_BYTES)
        msg.params.foreach { case (k, v) =>
          w.writeStringNul(k)
          w.writeStringNul(v)
        }
        w.writeByte(0)
        Encoded(None, w.toByteArray)

      case msg: PasswordMessage =>
        w.writeStringNul(msg.password)
        Encoded(Some('p'), w.toByteArray)

      case msg: Parse =>
        w.writeStringNul(msg.name)
        w.writeStringNul(msg.query)
        w.writeInt16(msg.types.length.toShort)
        msg.types.foreach { oid => w.writeInt32(oid.toInt) }
        Encoded(Some('P'), w.toByteArray)

      case msg: Describe =>
        // TODO: 'P' to describe portal
        w.writeChar8('S') // Statement
        w.writeStringNul(msg.name)
        Encoded(Some('D'), w.toByteArray)

      case msg: Bind =>
        w.writeStringNul(msg.portal)
        w.writeStringNul(msg.name)
        w.writeInt16(msg.binaryColumns.length.toShort)
        msg.binaryColumns.foreach { bc => w.writeInt16(if (bc) 1 else 0) }
        w.writeInt16(msg.params.length.toShort)
        msg.params.foreach { v =>
          w.writeInt32(v.length)
          w.writeBytes(v.encode)
        }
        w.writeInt16(0) // TODO: reset-column format codes
        Encoded(Some('B'), w.toByteArray)

      case msg: Execute =>
        w.writeStringNul(msg.portal)
        w.writeInt32(msg.maxRows)
        Encoded(Some('E'), w.toByteArray)

      case Flush => Encoded(Some('H'), Array.empty)
      case Sync => Encoded(Some('S'), Array.empty)
      case Terminate => Encoded(Some('X'), Array.empty)

      case msg =>
        throw new NotImplementedError(s"Not implemented: $msg")
    }
  }
}

object PGFrontendMessage {
  sealed case class StartupMessage(params: (String, String)*) extends PGFrontendMessage
  sealed case class CancelRequest(pid: Int, key: Int)
  sealed case class Bind(
    portal: String, name: String, params: List[PGValue], binaryColumns: List[Boolean]
  ) extends PGFrontendMessage
  sealed case class Close(name: String) extends PGFrontendMessage
  sealed case class Describe(name: String) extends PGFrontendMessage
  sealed case class Execute(portal: String, maxRows: Int) extends PGFrontendMessage
  case object Flush extends PGFrontendMessage
  sealed case class Parse(
    name: String, query: String, types: List[OID]
  ) extends PGFrontendMessage
  sealed case class PasswordMessage(password: Array[Byte]) extends PGFrontendMessage
  sealed case class SimpleQuery(query: String) extends PGFrontendMessage
  case object Sync extends PGFrontendMessage
  case object Terminate extends PGFrontendMessage

  final case class Encoded(id: Option[Char], body: Array[Byte])
}
