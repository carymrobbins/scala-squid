package squid.protocol

import java.nio.charset.StandardCharsets

/**
  * Base object for creating an instance of a PGConnection.  It is preferable to use
  * .withConnection as it will handle closing the connection for you.  When interacting
  * with the connection, you can use the methods provided on the PGConnection instance.
  */
object PGProtocol {
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
}

/**
  * Result from the protocol after executing the 'describe' command for a prepared statement.
  * This provides result column metadata (including types) and a parse tree of the statement.
  */
final case class DescribeResult(
  paramTypes: List[OID],
  columns: List[DescribeColumn],
  parseTree: String
)

/** Result column metadata returned from a 'describe' command. */
final case class DescribeColumn(name: String, colType: OID, nullable: Boolean)

/** The exception class raised from PGConnection upon protocol errors. */
final case class PGProtocolError(override val getMessage: String) extends Exception

object PGProtocolError {
  def fromErrorResponse(err: PGBackendMessage.ErrorResponse): PGProtocolError = {
    PGProtocolError(err.getMessage)
  }

  def fromPGResponseError(err: PGResponse.Error): PGProtocolError = {
    PGProtocolError(err.toString)
  }
}
