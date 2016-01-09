package squid.protocol

import scala.annotation.tailrec

import squid.util.BinaryReader

/** Various messages sent from the backend to the frontend. */
sealed trait PGBackendMessage

object PGBackendMessage {
  case object AuthenticationOk extends PGBackendMessage
  case object AuthenticationCleartextPassword extends PGBackendMessage
  sealed case class AuthenticationMD5Password(salt: Array[Byte]) extends PGBackendMessage
  sealed case class BackendKeyData(pid: Int, key: Int) extends PGBackendMessage
  case object BindComplete extends PGBackendMessage
  case object CloseComplete extends PGBackendMessage
  sealed case class CommandComplete(tag: String) extends PGBackendMessage
  sealed case class DataRow(columns: List[PGValue]) extends PGBackendMessage
  case object EmptyQueryResponse extends PGBackendMessage
  case object NoData extends PGBackendMessage
  sealed case class NoticeResponse(messageFields: MessageFields) extends PGBackendMessage
  sealed case class ParameterDescription(paramTypes: List[OID]) extends PGBackendMessage
  sealed case class ParameterStatus(name: String, value: String) extends PGBackendMessage
  case object ParseComplete extends PGBackendMessage
  case object PortalSuspended extends PGBackendMessage
  sealed case class ReadyForQuery(state: PGState) extends PGBackendMessage
  sealed case class RowDescription(columns: List[ColDescription]) extends PGBackendMessage

  sealed case class ErrorResponse(messageFields: MessageFields) extends PGBackendMessage {
    /** Attempts to get the error message from messageFields, otherwise uses toString */
    lazy val getMessage: String = {
      messageFields.toMap.get('M').map { m =>
        val hint = messageFields.toMap.get('H').map(h => s"\nHINT: $h").getOrElse("")
        s"ERROR: $m" + hint
      }.getOrElse(toString)
    }

    /** Pretty print error messages for SQL statements.  */
    def getMessageForSQL(sql: String): String = {
      val sqlMessage = messageFields.toMap.get('P').flatMap { p =>
        try {
          Some(p.toInt)
        } catch {
          case _: NumberFormatException => None
        }
      }.map { pos =>
        // Extract the appropriate line from the SQL
        val mbStart = sql.lastIndexOf('\n', pos)
        val start = if (mbStart == -1) 0 else mbStart + 1
        val mbEnd = sql.indexOf('\n', pos)
        val end = if (mbEnd == -1) sql.length - 1 else mbEnd
        if (end < 0) {
          ""
        } else {
          val sqlLine = sql.substring(start, end)
          val caret = (" " * (pos - start - 1)) + "^"
          s"\nSQL: $sqlLine\n     $caret"
        }
      }.getOrElse("")
      getMessage + sqlMessage
    }
  }


  /** Decode a backend message from binary. */
  def decode(r: BinaryReader): PGBackendMessage = {
    val id = r.readChar8()
    val startPos = r.getReadCount
    val len = r.readBigInt32()
    val msg = decodeBody(id, r)
    val endPos = r.getReadCount
    val remaining = len - (endPos - startPos)
    if (remaining > 0) r.skip(remaining.toLong)
    if (remaining < 0) throw new RuntimeException("Decoder overran message")
    msg
  }

  private def decodeBody(id: Char, r: BinaryReader): PGBackendMessage = id match {
    case 'R' => handleAuth(r.readInt32(), r)
    case 't' => ParameterDescription(r.rep(r.readInt16(), OID.decode).toList)
    case 'T' => RowDescription(r.rep(r.readInt16(), ColDescription.decode).toList)
    case 'Z' => ReadyForQuery(PGState.decode(r))
    case '1' => ParseComplete
    case '2' => BindComplete
    case '3' => CloseComplete
    case 'C' => CommandComplete(r.readStringNul())
    case 'S' => ParameterStatus(name = r.readStringNul(), value = r.readStringNul())
    case 'D' => DataRow(r.rep(r.readInt16(), PGValue.decode).toList)
    case 'K' => BackendKeyData(pid = r.readInt32(), key = r.readInt32())
    case 'E' => ErrorResponse(MessageFields.decode(r))
    case 'I' => EmptyQueryResponse
    case 'n' => NoData
    case 's' => PortalSuspended
    case 'N' => NoticeResponse(MessageFields.decode(r))
    case other => throw new RuntimeException(s"Unexpected backend message code: $other")
  }

  private def handleAuth(code: Int, r: BinaryReader): PGBackendMessage = code match {
    case 0 => AuthenticationOk
    case 3 => AuthenticationCleartextPassword
    case 5 => AuthenticationMD5Password(salt = r.read(4))
    case _ => throw new RuntimeException(s"Unexpected auth code: $code")
  }
}

/** Backend response describing a result column from the 'describe' command. */
final case class ColDescription(
  name: String,
  table: OID,
  number: Int,
  colType: OID,
  modifier: Int,
  binary: Boolean
)

object ColDescription {
  /** Decodes a ColDescription from binary. */
  def decode(r: BinaryReader): ColDescription = {
    val name = r.readStringNul()
    val table = OID(r.readInt32())
    val number = r.readInt16()
    val colType = OID(r.readInt32())
    r.readInt16() // type size
    val modifier = r.readInt32()
    val binary = r.readInt16() match {
      case 0 => true
      case 1 => false
      case other => throw new RuntimeException(s"Unexpected format code: $other")
    }
    ColDescription(name, table, number, colType, modifier, binary)
  }
}

/** Set of message fields used by certain backend messages. */
final case class MessageFields(toMap: Map[Char, String]) extends AnyVal
object MessageFields {
  def decode(r: BinaryReader): MessageFields = {
    @tailrec
    def loop(m: Map[Char, String]): Map[Char, String] = r.readChar8() match {
      case 0 => m
      case k => loop(m.updated(k, r.readStringNul()))
    }
    MessageFields(loop(Map.empty))
  }
}
