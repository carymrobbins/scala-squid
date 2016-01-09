package squid.protocol

import scala.io.AnsiColor

/** Simple logger if our PGConnectInfo is set to debug. */
final class PGLogger(debug: Boolean) {
  def log(msg: Any, received: Boolean = false, sent: Boolean = false): Unit = {
    if (debug) {
      if (received) {
        println(s"${AnsiColor.BLUE}<<< $msg${AnsiColor.RESET}")
      } else if (sent) {
        println(s"${AnsiColor.MAGENTA}>>> $msg${AnsiColor.RESET}")
      } else {
        println(s"${AnsiColor.YELLOW}LOG: $msg${AnsiColor.RESET}")
      }
    }
  }
}
