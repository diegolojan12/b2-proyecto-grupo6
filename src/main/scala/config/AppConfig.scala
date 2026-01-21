package config

import com.typesafe.config.ConfigFactory

object AppConfig {

  private val root = ConfigFactory.load()

  // Para slf4j-simple: necesita System property (no basta con que esté en HOCON)
  def initLogging(): Unit = {
    if (root.hasPath("org.slf4j.simpleLogger.defaultLogLevel")) {
      val level = root.getString("org.slf4j.simpleLogger.defaultLogLevel").replace("\"", "")
      System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", level)
    }
  }

  object db {
    private val c = root.getConfig("db")
    val driver: String   = c.getString("driver")
    val url: String      = c.getString("url")
    val user: String     = c.getString("user")
    val password: String = c.getString("password")
  }
}
