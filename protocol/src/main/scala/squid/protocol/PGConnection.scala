package squid.protocol

import scala.annotation.tailrec

import PGBackendMessage._
import PGFrontendMessage._

/** A connection to the PostgreSQL protocol backend. */
final class PGConnection(info: PGConnectInfo) {

  // Lazy to ensure socket has been initialized.
  lazy val query = new PGQueryExecutor(socket)
  lazy val types = new PGTypesManager(query)
  lazy val tables = new PGTablesManager(query)
  lazy val operators = new PGOperatorsManager(query)

  /** Returns the current state of the connection. */
  def state: PGState = stateMgr.get

  /**
    * Connects to the backend.  This should not be called directly.
    * Instead, use PGProtocol.withConnection
    */
  def connect(): Unit = {
    if (socket.isConnected) throw new RuntimeException("Already connected")
    logger.log(s"Connecting to postgresql ${info.host}:${info.port}")
    socket.connect()
    socket.send(startupMessage)
    socket.flush()
    doAuth()
    readStartupMessages()
  }

  /**
    * Disconnects from the backend.  This should not be called directly.
    * Instead, use PGProtocol.withConnection
    */
  def close(): Unit = {
    socket.send(Terminate)
    socket.close()
  }

  /** Called on .connect to initialize the connection state. */
  private def readStartupMessages(): Unit = {
    @tailrec
    def loop(): Unit = {
      socket.receive().getOrThrow.getOrElse {
        throw new RuntimeException("No message received from backend")
      } match {
        case msg: ParameterStatus =>
          paramStatusMgr.update(msg)
          loop()

        case msg: BackendKeyData =>
          backendKeyMgr.update(msg)
          loop()

        case msg: NoticeResponse =>
          logger.log("ignoring...")
          loop()

        case msg: ReadyForQuery =>
          stateMgr.update(msg)

        case other =>
          throw new RuntimeException(s"Unexpected startup message from backend: $other")
      }
    }
    loop()
  }

  /**
    * Returns an encoded password from the provided info via the class constructor given the salt
    * retrieved from the backend.
    *
    * See the AuthenticationMD5Password section of the protocol flow docs for more info.
    * http://www.postgresql.org/docs/9.4/static/protocol-flow.html
    */
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

  /** Runs the authorization workflow on .connect */
  private def doAuth(): Unit = {
    @tailrec
    def loop(): Unit = {
      socket.receive().getOrThrow.getOrElse {
        throw new RuntimeException("No message received from backend")
      } match {
        case msg@AuthenticationMD5Password(salt) =>
          socket.send(PasswordMessage(encodePassword(salt)))
          socket.flush()
          loop()

        case AuthenticationOk => // Done

        case other =>
          throw new RuntimeException(s"Unexpected response from backend: $other")
      }
    }
    loop()
  }

  /** The initial startup message sent upon .connect */
  private lazy val startupMessage = StartupMessage(
    "user" -> info.user,
    "database" -> info.database,
    "client_encoding" -> "UTF8",
    "standard_conforming_strings" -> "on",
    "bytea_output" -> "hex",
    "DateStyle" -> "ISO, YMD",
    "IntervalStyle" -> "iso_8601",
    // These allow us to retrieve parse trees via NoticeResponse
    "client_min_messages" -> "debug1",
    "debug_print_parse" -> "on",
    "debug_pretty_print" -> (if (info.prettyPrintParseTrees) "on" else "off")
  )

  private val logger = new PGLogger(info.debug)
  private val stateMgr = new PGStateManager
  private val socket = new PGSocket(info, stateMgr, logger)
  private val backendKeyMgr = new PGBackendKeyManager
  private val paramStatusMgr = new PGParamStatusManager
}
