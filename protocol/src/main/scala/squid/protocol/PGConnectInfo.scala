package squid.protocol

import com.typesafe.config.Config

/** Wrapper for connection info, normally supplied by a typesafe config file. */
final case class PGConnectInfo(
  host: String,
  port: Int,
  timeout: Int,
  user: String,
  password: String,
  database: String,
  debug: Boolean = false,
  prettyPrintParseTrees: Boolean = false
)

object PGConnectInfo {
  def fromConfig(config: Config): PGConnectInfo = PGConnectInfo(
    host = config.getString("squid.protocol.host"),
    port = config.getInt("squid.protocol.port"),
    timeout = config.getInt("squid.protocol.timeout"),
    user = config.getString("squid.protocol.username"),
    password = config.getString("squid.protocol.password"),
    database = config.getString("squid.protocol.database")
  )
}
