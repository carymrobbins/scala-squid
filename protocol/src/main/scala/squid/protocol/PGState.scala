package squid.protocol

import squid.util.BinaryReader

/** Various states for the protocol. */
sealed trait PGState

object PGState {
  case object Unknown extends PGState // no Sync
  case object Command extends PGState // was Sync, sent Command
  case object Pending extends PGState // Sync sent
  case object Idle extends PGState
  case object Transaction extends PGState
  case object TransactionFailed extends PGState
  case object Closed extends PGState // Terminate sent or EOF received

  /** Decodes the state from binary. */
  def decode(r: BinaryReader): PGState = r.readChar8() match {
    case 'I' => Idle
    case 'T' => Transaction
    case 'E' => TransactionFailed
    case other => throw new RuntimeException(s"Unexpected state code: $other")
  }
}
