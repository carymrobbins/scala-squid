package squid.protocol

/** Manages the state of the backend pid and key. */
final class PGBackendKeyManager {

  def getPid: Int = pid

  def getKey: Int = key

  def update(msg: PGBackendMessage.BackendKeyData): Unit = {
    pid = msg.pid
    key = msg.key
  }

  private[this] var pid: Int = -1
  private[this] var key: Int = -1
}
