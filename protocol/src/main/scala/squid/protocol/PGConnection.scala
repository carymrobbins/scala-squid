package squid.protocol

import java.io.{InputStream, BufferedInputStream, OutputStream, BufferedOutputStream}
import java.net.{InetSocketAddress, Socket}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.AnsiColor

import squid.util.{BinaryReader, BinaryWriter}

/** A connection to the PostgreSQL protocol backend. */
final class PGConnection(info: PGConnectInfo) {
  import PGBackendMessage._
  import PGFrontendMessage._

  /** Describes a prepared statement given its SQL string and parameter types (optional). */
  def describe(sql: String, types: List[OID]): DescribeResult = {
    sync()
    send(Parse("", sql, types))
    send(Describe(""))
    send(Flush)
    send(Sync)
    flush()

    @tailrec
    def waitForParseTree(): String = receive() match {
      case Some(NoticeResponse(fields)) =>
        fields.toMap.get('D') match {
          case Some(result) => result // Found parse tree
          case None => waitForParseTree() // Ignore and recurse, waiting for parse tree
        }

      case Some(err: ErrorResponse) => throw PGProtocolError(err)
      case other => throw new RuntimeException(s"Expected NoticeResponse, got: $other")
    }

    val parseTree = waitForParseTree()
    receive() match {
      case Some(ParseComplete) => // Skip
      case other => throw new RuntimeException(s"Expected ParseComplete, got: $other")
    }
    val paramTypes = receive() match {
      case Some(ParameterDescription(pts)) => pts
      case other => throw new RuntimeException(s"Expected ParameterDescription, got: $other")
    }
    val cols = receive() match {
      case Some(NoData) => List()
      case Some(RowDescription(cds)) =>
        cds.map { cd =>
          val nullable = columnIsNullable(cd.table, cd.number)
          DescribeColumn(cd.name, cd.colType, nullable)
        }
      case other => throw new RuntimeException(s"Expected RowDescription, got: $other")
    }
    DescribeResult(paramTypes, cols, parseTree)
  }

  /** Executes a prepared query and returns its results int a list. */
  def preparedQuery
      (query: String, params: List[PGParam], binaryCols: List[Boolean] = Nil)
      : List[List[PGValue]] = {
    bind(query, params, binaryCols)
    send(Execute("", 0)) // zero for "fetch all rows"
    send(Flush)
    send(Sync)
    flush()
    new Iterator[List[PGValue]] {
      private var nextMsg = receive()

      override def hasNext: Boolean = nextMsg match {
        case Some(EmptyQueryResponse) => false
        case Some(_: CommandComplete) => false
        case _ => true
      }

      override def next(): List[PGValue] = nextMsg match {
        case Some(DataRow(values)) =>
          nextMsg = receive()
          values

        case other => throw new RuntimeException(s"Unexpected row message: $other")
      }
    }.toList
  }

  /** Gets the system OID for the provided table. */
  def getTableOID(schema: String, table: String): Option[OID] = {
    tableOIDs.get((schema, table)) match {
      case Some(oid) => Some(oid)

      case None =>
        preparedQuery(
          """
            select pg_class.oid
            from pg_catalog.pg_class, pg_catalog.pg_namespace
            where pg_class.relnamespace = pg_namespace.oid and
                  pg_namespace.nspname = $1 and
                  pg_class.relname = $2
          """,
          List(PGParam.from(schema), PGParam.from(table))
        ).flatten match {
          case List(v) =>
            val oid = v.as[OID]
            tableOIDs.update((schema, table), oid)
            Some(oid)

          case Nil => None
          case other => throw new RuntimeException(s"Expected zero or one element, got: $other")
        }
    }
  }

  /**
    * Gets the system OID for the respective Scala type, given the Scala type has an instance
    * of the PGType typeclass.
    */
  //noinspection AccessorLikeMethodIsEmptyParen
  def getTypeOID[A : PGType](): OID = {
    val (namespace, typeName) = PGType.tupled[A]
    getTypeOID(namespace, typeName)
  }

  /** Gets the system OID for a type given its qualified name. */
  def getTypeOID(namespace: String, typeName: String): OID = {
    typeOIDs.get((namespace, typeName)) match {
      case Some(oid) => oid

      case None =>
        preparedQuery(
          """
            select pg_type.oid
            from pg_catalog.pg_type, pg_catalog.pg_namespace
            where pg_type.typnamespace = pg_namespace.oid and
                  pg_namespace.nspname = $1 and
                  pg_type.typname = $2
          """,
          List(PGParam.from(namespace), PGParam.from(typeName))
        ).flatten match {
          case List(v) =>
            val oid = v.as[OID]
            cacheTypeOID(namespace, typeName, oid)
            oid

          case Nil => throw new RuntimeException(s"pg_type not found: $namespace.$typeName")
          case other => throw new RuntimeException(s"Expected one element, got: $other")
        }
    }
  }

  /** Gets the name of a type given its OID. */
  def getTypeName(oid: OID): PGTypeName = {
    oidTypes.get(oid) match {
      case Some((namespace, typeName)) => PGTypeName(namespace, typeName)

      case None =>
        preparedQuery(
          """
            select pg_namespace.nspname, pg_type.typname
            from pg_catalog.pg_type, pg_catalog.pg_namespace
            where pg_type.typnamespace = pg_namespace.oid and
                  pg_type.oid = $1
          """,
          List(PGParam.from(oid)),
          binaryCols = Nil
        ).flatten match {
          case List(pgNamespace, pgTypeName) =>
            val namespace = pgNamespace.as[String]
            val typeName = pgTypeName.as[String]
            cacheTypeOID(namespace, typeName, oid)
            PGTypeName(namespace, typeName)

          case Nil => throw new RuntimeException(s"pg_type not found: $oid")
          case xs => throw new RuntimeException(s"Expected only two elements, got: $xs")
        }
    }
  }

  /** Gets the qualified operator name for the given OID. */
  def getOpName(oid: OID): PGOp = {
    oidOps.get(oid) match {
      case Some((namespace, op)) => PGOp(namespace, op)

      case None =>
        preparedQuery(
          """
            select pg_namespace.nspname, pg_operator.oprname
            from pg_catalog.pg_operator, pg_catalog.pg_namespace
            where pg_operator.oprnamespace = pg_namespace.oid and
                  pg_operator.oid = $1
          """,
          List(PGParam.from(oid)),
          binaryCols = Nil
        ).flatten match {
          case List(pgNamespace, pgOp) =>
            val namespace = pgNamespace.as[String]
            val op = pgOp.as[String]
            cacheOpOID(namespace, op, oid)
            PGOp(namespace, op)

          case Nil => throw new RuntimeException(s"pg_operator not found: $oid")
          case other => throw new RuntimeException(s"Expected only two elements, got: $other")
        }
    }
  }

  /** Returns the current state of the connection. */
  def getState: PGState = state

  /**
    * Connects to the backend.  This should not be called directly.
    * Instead, use PGProtocol.withConnection
    */
  def connect(): Unit = {
    if (socket.isConnected) throw new RuntimeException("Already connected")
    log(s"Connecting to postgresql ${info.host}:${info.port}")
    socket.connect(new InetSocketAddress(info.host, info.port), info.timeout)
    send(startupMessage)
    flush()
    doAuth()
    readStartupMessages()
  }

  /**
    * Disconnects from the backend.  This should not be called directly.
    * Instead, use PGProtocol.withConnection
    */
  def close(): Unit = {
    send(Terminate)
    socket.close()
  }

  /** Sends a low-level message to the backend. */
  private def send(msg: PGFrontendMessage): Unit = {
    state = getNewState(msg)
    log(msg, sent = true)
    val encoded = msg.encode
    val w = new BinaryWriter(PGProtocol.CHARSET)
    encoded.id.foreach { w.writeChar8 }
    w.writeInt32(encoded.body.length + 4)
    w.writeBytes(encoded.body)
    out.write(w.toByteArray)
  }

  /** Flushes messages sent to the backend. */
  private def flush(): Unit = out.flush()

  /** Attempts to receive a low-level message from the backend. */
  private def receive(): Option[PGBackendMessage] = {
    val r = new BinaryReader(in, PGProtocol.CHARSET)
    if (r.peek() == -1) {
      None
    } else {
      val msg = PGBackendMessage.decode(r)
      log(msg, received = true)
      Some(msg)
    }
  }

  /** Attempts to sync with the backend. */
  private def sync(): Unit = state match {
    case PGState.Closed => throw new RuntimeException("The connection is closed")
    case PGState.Pending | PGState.Unknown => syncWait()
    case _ => log(s"Sync ok, state is $state")
  }

  /** Binds a prepared statement. */
  private def bind(query: String, params: List[PGParam], binaryCols: List[Boolean]): Unit = {
    sync()
    val paramTypes = params.flatMap(_.typeOID)
    val key = (query, paramTypes)
    val id = preparedStatements.getOrElse(key, {
      val id = preparedStatements.size + 1
      send(Parse(name = id.toString, query, paramTypes))
      id
    })
    val paramValues = params.map(_.value)
    send(Bind(portal = "", name = id.toString, paramValues, binaryCols))
    send(Flush) // TODO: Why do we have to send a Flush here?
    flush()

    while (true) {
      receive() match {
        case Some(ParseComplete) => preparedStatements.update(key, id)
        case Some(_: NoticeResponse) => // Parse tree, ignored.
        case Some(BindComplete) => return
        case other => throw new RuntimeException(s"Unexpected response: $other")
      }
    }
  }

  /** Called by .sync to wait until we are synced with the backend. */
  @tailrec
  private def syncWait(): Unit = {
    receive() match {
      case None =>
        send(Sync)
        flush()
        syncWait()

      case Some(_: ReadyForQuery) => // Done
      case Some(msg) => throw new RuntimeException(s"Unexpected message during sync: $msg")
    }
  }

  /** Called on .connect to initialize the connection state. */
  private def readStartupMessages(): Unit = {
    @tailrec
    def loop(): Unit = {
      receive().getOrElse {
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

  /**
    * Returns an encoded password from the provided info via the class constructor given the salt
    * retrieved from the backend.
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
      receive().getOrElse {
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

  /** Determine the next connection state from the given message and our current state. */
  private def getNewState(msg: PGFrontendMessage): PGState = (msg, state) match {
    case (_, PGState.Closed) => PGState.Closed
    case (PGFrontendMessage.Sync, _) => PGState.Pending
    case (PGFrontendMessage.Terminate, _) => PGState.Closed
    case (_, PGState.Unknown) => PGState.Unknown
    case _ => PGState.Command
  }

  /** Infers column nullability by checking the pg_attribute table. */
  private def columnIsNullable(table: OID, colNum: Int): Boolean = {
    if (table.toInt == 0) {
      true
    } else {
      columnNullables.get((table, colNum)) match {
        case Some(nullable) => nullable

        case None =>
          val result = preparedQuery(
            "select attnotnull from pg_catalog.pg_attribute where attrelid = $1 and attnum = $2",
            List(
              PGParam.from(table).typed(getTypeOID[OID]()),
              PGParam.from(colNum).typed(getTypeOID[Int]())
            ),
            binaryCols = Nil
          )
          val nullable = result.flatten.headOption.map(v => !v.as[Boolean]).getOrElse(true)
          columnNullables.update((table, colNum), nullable)
          nullable
      }
    }
  }

  /** Caches type OIDs to avoid asking the protocol multiple times. */
  private def cacheTypeOID(namespace: String, typeName: String, oid: OID): Unit = {
    typeOIDs.update((namespace, typeName), oid)
    oidTypes.update(oid, (namespace, typeName))
  }

  private def cacheOpOID(namespace: String, op: String, oid: OID): Unit = {
    opOIDs.update((namespace, op), oid)
    oidOps.update(oid, (namespace, op))
  }

  /** Simple logger if our PGConnectInfo is set to debug. */
  private def log(msg: Any, received: Boolean = false, sent: Boolean = false): Unit = {
    if (info.debug) {
      if (received) {
        println(s"${AnsiColor.BLUE}<<< $msg${AnsiColor.RESET}")
      } else if (sent) {
        println(s"${AnsiColor.MAGENTA}>>> $msg${AnsiColor.RESET}")
      } else {
        println(s"${AnsiColor.YELLOW}LOG: $msg${AnsiColor.RESET}")
      }
    }
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

  private[this] var state: PGState = PGState.Unknown
  private[this] var pid: Int = -1
  private[this] var key: Int = -1
  private val params = mutable.Map.empty[String, String]
  private val preparedStatements = mutable.Map.empty[(String, List[OID]), Int]
  private val columnNullables = mutable.Map.empty[(OID, Int), Boolean]
  private val typeOIDs = mutable.Map.empty[(String, String), OID]
  private val oidTypes = mutable.Map.empty[OID, (String, String)]
  private val opOIDs = mutable.Map.empty[(String, String), OID]
  private val oidOps = mutable.Map.empty[OID, (String, String)]
  private val tableOIDs = mutable.Map.empty[(String, String), OID]
  private val socket = new Socket()
  private lazy val in: InputStream = new BufferedInputStream(socket.getInputStream, 8192)
  private lazy val out: OutputStream = new BufferedOutputStream(socket.getOutputStream, 8192)
}
