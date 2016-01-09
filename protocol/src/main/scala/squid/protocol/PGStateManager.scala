package squid.protocol

import PGBackendMessage._

/** Manages the state of the PostgreSQL protocol connection. */
final class PGStateManager {

  def get: PGState = state

  def update(msg: PGFrontendMessage): Unit = {
    state = nextState(msg)
  }

  def update(msg: ReadyForQuery): Unit = {
    state = msg.state
  }

  /** Determine the next connection state from the given message and our current state. */
  private def nextState(msg: PGFrontendMessage): PGState = (msg, state) match {
    case (_, PGState.Closed) => PGState.Closed
    case (PGFrontendMessage.Sync, _) => PGState.Pending
    case (PGFrontendMessage.Terminate, _) => PGState.Closed
    case (_, PGState.Unknown) => PGState.Unknown
    case _ => PGState.Command
  }

  private[this] var state: PGState = PGState.Unknown
}
