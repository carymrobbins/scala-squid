package squid.protocol

import java.io._
import java.net.{InetSocketAddress, Socket}
import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.AnsiColor

object PGProtocol {
  import PGFrontendMessage._, PGBackendMessage._

  val CHARSET = StandardCharsets.UTF_8
  val PROTOCOL_MAJOR: Byte = 3
  val PROTOCOL_MINOR: Byte = 0
  val PROTOCOL_BYTES: Array[Byte] = Array(0: Byte, PROTOCOL_MAJOR, 0: Byte, PROTOCOL_MINOR)

  def withConnection[A](info: PGConnectInfo)(block: PGConnection => A): A = {
    val c = getConnection(info)
    try {
      block(c)
    } finally {
      c.close()
    }
  }

  def getConnection(info: PGConnectInfo): PGConnection = {
    val c = new PGConnection(info)
    c.connect()
    c
  }

  def describe(sql: String, types: List[OID])(implicit c: PGConnection) = {
    c.sync()
    c.send(Parse("", sql, types))
    c.send(Describe(""))
    c.send(Flush)
    c.send(Sync)
    c.flush()
    c.receive(true) match {
      case Some(ParseComplete) => ()
      case other => throw new RuntimeException(s"Expected ParseComplete, got: $other")
    }
    val paramTypes = c.receive(true) match {
      case Some(ParameterDescription(pts)) => pts
      case other => throw new RuntimeException(s"Expected ParameterDescription, got: $other")
    }
    val cols = c.receive(true) match {
      case Some(NoData) => List()
      case Some(RowDescription(cds)) =>
        cds.map { cd =>
          // TODO: Check nullability via query
          val nullable = true
          DescribeColumn(cd.name, cd.colType, nullable)
        }
      case other => throw new RuntimeException(s"Expected RowDescription, got: $other")
    }
    DescribeResult(paramTypes, cols)
  }
}

final case class DescribeResult(
  paramTypes: List[OID],
  columns: List[DescribeColumn]
)

final case class DescribeColumn(name: String, colType: OID, nullable: Boolean)

final case class PGProtocolError(msg: PGBackendMessage.ErrorResponse) extends Exception {
  override def getMessage: String = msg.toString
}

final class PGConnection(info: PGConnectInfo) {
  import PGFrontendMessage._, PGBackendMessage._

  def send(msg: PGFrontendMessage): Unit = {
    state = getNewState(msg)
    log(msg, sent = true)
    val encoded = msg.encode
    val w = new BinaryWriter(PGProtocol.CHARSET)
    encoded.id.foreach { w.writeChar8 }
    w.writeInt32(encoded.body.length + 4)
    w.writeBytes(encoded.body)
    out.write(w.toByteArray)
  }

  def flush(): Unit = out.flush()

  def receive(blocking: Boolean): Option[PGBackendMessage] = {
    val r = new BinaryReader(in, PGProtocol.CHARSET)
    if (r.peek() == -1) {
      None
    } else {
      val msg = PGBackendMessage.decode(r)
      log(msg, received = true)
      Some(msg)
    }
  }

  def sync(): Unit = state match {
    case PGState.Closed => throw new RuntimeException("The connection is closed")
    case PGState.Pending => syncWait(true)
    case PGState.Unknown => syncWait(false)
    case _ => ()
  }

  def connect(): Unit = {
    log(s"Connecting to postgresql ${info.host}:${info.port}")
    socket.connect(new InetSocketAddress(info.host, info.port), info.timeout)
    send(startupMessage(info))
    flush()
    doAuth()
    readStartupMessages()
  }

  def close(): Unit = {
    in.close()
    out.close()
    socket.close()
  }

  def getState: PGState = state

  @tailrec
  private def syncWait(block: Boolean): Unit = {
    receive(block) match {
      case None =>
        send(Sync)
        flush()
        syncWait(true)

      case Some(err: ErrorResponse) =>
        syncWait(block)

      case Some(msg: ReadyForQuery) => ()

      case Some(msg) =>
        throw new RuntimeException(s"Unexpected message during sync: $msg")
    }
  }

  private def readStartupMessages(): Unit = {
    @tailrec
    def loop(): Unit = {
      receive(true).getOrElse {
        throw new RuntimeException("No message received from backend")
      } match {
        case err: ErrorResponse => throw PGProtocolError(err)

        case msg: ParameterStatus =>
          params(msg.name) = msg.value
          loop()

        case msg: BackendKeyData =>
          pid = msg.pid
          key = msg.key
          loop()

        case msg: NoticeResponse =>
          log("ignoring...")
          loop()

        case ReadyForQuery(s) =>
          state = s
          // Done

        case other =>
          throw new RuntimeException(s"Unexpected startup message from backend: $other")
      }
    }
    loop()
  }

  private def encodePassword(salt: Array[Byte]): Array[Byte] = {
    val md = java.security.MessageDigest.getInstance("MD5")

    def md5(bs: Array[Byte]): Array[Byte] = {
      md.reset()
      md.update(bs)
      val digest = md.digest()
      val hash = BigInt(1, digest).toString(16)
      (("0" * (32 - hash.length)) + hash).getBytes(PGProtocol.CHARSET)
    }

    val prefix = "md5".getBytes(PGProtocol.CHARSET)
    val user = info.user.getBytes(PGProtocol.CHARSET)
    val pass = info.password.getBytes(PGProtocol.CHARSET)
    prefix ++ md5(md5(pass ++ user) ++ salt)
  }

  private def doAuth(): Unit = {
    @tailrec
    def loop(): Unit = {
      receive(true).getOrElse {
        throw new RuntimeException("No message received from backend")
      } match {
        case err: ErrorResponse => throw PGProtocolError(err)

        case msg@AuthenticationMD5Password(salt) =>
          send(PasswordMessage(encodePassword(salt)))
          flush()
          loop()

        case AuthenticationOk => // Done

        case other =>
          throw new RuntimeException(s"Unexpected response from backend: $other")
      }
    }
    loop()
  }

  private def getNewState(msg: PGFrontendMessage): PGState = (msg, state) match {
    case (_, PGState.Closed) => PGState.Closed
    case (PGFrontendMessage.Sync, _) => PGState.Pending
    case (PGFrontendMessage.Terminate, _) => PGState.Closed
    case (_, PGState.Unknown) => PGState.Unknown
    case _ => PGState.Command
  }

  private def writeByte(c: Char): Unit = out.write(c.toByte)

  private def writeBytes4(n: Int): Unit = out.write(ByteBuffer.allocate(4).putInt(n).array())

  private def writeBytes(bs: Array[Byte]): Unit = out.write(bs)

  private def log(msg: Any, received: Boolean = false, sent: Boolean = false): Unit = {
    if (info.debug) {
      if (received) {
        println(s"${AnsiColor.BLUE_B}<<< $msg${AnsiColor.RESET}")
      } else if (sent) {
        println(s"${AnsiColor.MAGENTA_B}>>> $msg${AnsiColor.RESET}")
      } else {
        println(s"${AnsiColor.YELLOW}LOG: $msg${AnsiColor.RESET}")
      }
    }
  }

  private def startupMessage(info: PGConnectInfo) = StartupMessage(
    "user" -> info.user,
    "database" -> info.database,
    "client_encoding" -> "UTF8",
    "standard_conforming_strings" -> "on",
    "bytea_output" -> "hex",
    "DateStyle" -> "ISO, YMD",
    "IntervalStyle" -> "iso_8601"
  )

  private[this] var state: PGState = PGState.Unknown
  private[this] var pid: Int = -1
  private[this] var key: Int = -1
  private[this] val params = mutable.Map.empty[String, String]
  private val socket = new Socket()
  private lazy val in: InputStream = new BufferedInputStream(socket.getInputStream, 8192)
  private lazy val out: OutputStream = new BufferedOutputStream(socket.getOutputStream, 8192)
}

final case class PGConnectInfo(
  host: String,
  port: Int,
  timeout: Int,
  user: String,
  password: String,
  database: String,
  debug: Boolean = false
)

sealed trait PGState
object PGState {
  case object Unknown extends PGState // no Sync
  case object Command extends PGState // was Sync, sent Command
  case object Pending extends PGState // Sync sent
  case object Idle extends PGState
  case object Transaction extends PGState
  case object TransactionFailed extends PGState
  case object Closed extends PGState // Terminate sent or EOF received

  def decode(r: BinaryReader): PGState = r.readChar8() match {
    case 'I' => Idle
    case 'T' => Transaction
    case 'E' => TransactionFailed
    case other => throw new RuntimeException(s"Unexpected state code: $other")
  }
}

sealed trait PGFrontendMessage {
  import PGFrontendMessage._

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

      case Flush =>
        Encoded(Some('H'), Array.empty)

      case Sync =>
        Encoded(Some('S'), Array.empty)

      case msg =>
        throw new NotImplementedError(s"Not implemented: $msg")
    }
  }
}

object PGFrontendMessage {
  sealed case class StartupMessage(params: (String, String)*) extends PGFrontendMessage
  sealed case class CancelRequest(pid: Int, key: Int)
  sealed case class Bind(
    name: String, params: List[PGValue], binaryColumns: List[Boolean]
  ) extends PGFrontendMessage
  sealed case class Close(name: String) extends PGFrontendMessage
  sealed case class Describe(name: String) extends PGFrontendMessage
  sealed case class Execute(maxRows: Int) extends PGFrontendMessage
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
  sealed case class ErrorResponse(messageFields: MessageFields) extends PGBackendMessage
  case object NoData extends PGBackendMessage
  sealed case class NoticeResponse(messageFields: MessageFields) extends PGBackendMessage
  sealed case class ParameterDescription(paramTypes: List[OID]) extends PGBackendMessage
  sealed case class ParameterStatus(name: String, value: String) extends PGBackendMessage
  case object ParseComplete extends PGBackendMessage
  case object PortalSuspended extends PGBackendMessage
  sealed case class ReadyForQuery(state: PGState) extends PGBackendMessage
  sealed case class RowDescription(columns: List[ColDescription]) extends PGBackendMessage

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

final case class ColDescription(
  name: String,
  table: OID,
  number: Int,
  colType: OID,
  modifier: Int,
  binary: Boolean
)

object ColDescription {
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

final case class OID(toInt: Int) extends AnyVal
object OID {
  def decode(r: BinaryReader): OID = OID(r.readInt32())
}

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

sealed trait PGValue
object PGValue {
  case object Null extends PGValue
  sealed case class Text(value: String) extends PGValue
  sealed case class Binary(value: Array[Byte]) extends PGValue

  def decode(r: BinaryReader): PGValue = r.readInt32() match {
    case 0xFFFFFFFF => PGValue.Null
    case len => PGValue.Text(r.readString(len))
  }
}

class BinaryReader(in: InputStream, charset: Charset) {

  def getReadCount: BigInt = readCount

  def peek(): Int = {
    in.mark(1)
    val result = in.read()
    in.reset()
    result
  }

  def skip(length: Long): Long = in.skip(length)

  def rep[A](n: Int, f: BinaryReader => A): Stream[A] = {
    if (n == 0) {
      Stream.empty
    } else {
      Stream.continually(f(this)).take(n)
    }
  }

  def read(): Byte = {
    val b = in.read()
    if (b == -1) throw new RuntimeException("Unexpected end of input")
    readCount += 1
    b.toByte
  }

  def read(length: Int): Array[Byte] = {
    val builder = new ByteArrayOutputStream(length)
    for (i <- 1 to length) {
      builder.write(read())
    }
    builder.toByteArray
  }

  def readString(length: Int): String = new String(read(length), charset)

  def readStringNul(): String = {
    val builder = new ByteArrayOutputStream()
    while (true) {
      val b = read()
      if (b == 0) return builder.toString(charset.name)
      builder.write(b)
    }
    throw new RuntimeException("Expected null terminator")
  }

  def readChar8(): Char = read().toChar

  def readInt16(): Short = ByteBuffer.wrap(read(2)).getShort()

  def readInt32(): Int = ByteBuffer.wrap(read(4)).getInt()

  def readBigInt32(): BigInt = BigInt(read(4))

  private[this] var readCount: BigInt = BigInt(0)
}

class BinaryWriter(charset: Charset) {

  def writeByte(b: Byte): Unit = builder.write(b)

  def writeBytes(bs: Array[Byte]): Unit = builder.write(bs)

  def writeChar8(c: Char): Unit = builder.write(c.toByte)

  def writeInt16(n: Short): Unit = builder.write(ByteBuffer.allocate(2).putShort(n).array())

  def writeInt32(n: Int): Unit = builder.write(ByteBuffer.allocate(4).putInt(n).array())

  def writeStringNul(s: Array[Byte]): Unit = {
    builder.write(s)
    builder.write(0)
  }

  def writeStringNul(s: String): Unit = writeStringNul(s.getBytes(charset))

  def toByteArray: Array[Byte] = builder.toByteArray

  private val builder = new ByteArrayOutputStream()
}
