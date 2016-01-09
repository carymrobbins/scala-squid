package squid.protocol

import scala.collection.mutable

/** Manages the state of the backend params. */
final class PGParamStatusManager {

  def update(msg: PGBackendMessage.ParameterStatus): Unit = {
    params(msg.name) = msg.value
  }

  private val params = mutable.Map.empty[String, String]
}
