package config

import cats.effect.{IO, Resource}
import doobie.util.transactor.Transactor
import java.util.Properties

object Database {

  def init(): Unit =
    AppConfig.initLogging()

  val transactor: Resource[IO, Transactor[IO]] = {
    val props = new Properties()
    props.setProperty("user", AppConfig.db.user)
    props.setProperty("password", AppConfig.db.password)

    Resource.pure(
      Transactor.fromDriverManager[IO](
        driver = AppConfig.db.driver,
        url    = AppConfig.db.url,
        info   = props,
        logHandler = None
      )
    )
  }
}