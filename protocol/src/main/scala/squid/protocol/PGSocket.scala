package squid.protocol

import java.io.{BufferedOutputStream, OutputStream, BufferedInputStream, InputStream}
import java.net.{Socket, InetSocketAddress}

import scala.annotation.tailrec

import squid.util.{BinaryReader, BinaryWriter}

/** Manages the underlying socket connection to the protocol backend. */
final class PGSocket(info: PGConnectInfo, stateMgr: PGStateManager, logger: PGLogger) {

  /** Sends a low-level message to the backend. */
  def send(msg: PGFrontendMessage): Unit = {
    stateMgr.update(msg)
    logger.log(msg, sent = true)
    val encoded = msg.encode
    val w = new BinaryWriter(PGProtocol.CHARSET)
    encoded.id.foreach { w.writeChar8 }
    w.writeInt32(encoded.body.length + 4)
    w.writeBytes(encoded.body)
    out.write(w.toByteArray)
  }

  /** Flushes messages sent to the backend. */
  def flush(): Unit = out.flush()

  /** Attempts to receive a low-level message from the backend. */
  def receive(): Option[PGBackendMessage] = {
    val r = new BinaryReader(in, PGProtocol.CHARSET)
    if (r.peek() == -1) {
      None
    } else {
      val msg = PGBackendMessage.decode(r)
      logger.log(msg, received = true)
      Some(msg)
    }
  }

  def connect(): Unit = socket.connect(new InetSocketAddress(info.host, info.port), info.timeout)

  def isConnected: Boolean = socket.isConnected

  def close(): Unit = socket.close()

  /** Attempts to sync with the backend. */
  def sync(): Unit = stateMgr.get match {
    case PGState.Closed => throw new RuntimeException("The connection is closed")
    case PGState.Pending | PGState.Unknown => syncWait()
    case other => logger.log(s"Sync ok, state is $other")
  }

  /** Called by .sync to wait until we are synced with the backend. */
  @tailrec
  private def syncWait(): Unit = {
    receive() match {
      case None =>
        send(PGFrontendMessage.Sync)
        flush()
        syncWait()

      case Some(_: PGBackendMessage.ReadyForQuery) => // Done
      case Some(msg) => throw new RuntimeException(s"Unexpected message during sync: $msg")
    }
  }

  private val socket = new Socket()
  private lazy val in: InputStream = new BufferedInputStream(socket.getInputStream, 8192)
  private lazy val out: OutputStream = new BufferedOutputStream(socket.getOutputStream, 8192)
}
