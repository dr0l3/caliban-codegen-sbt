import generated.{API, Extensions}
import runner.DefaultRunner
import zio._

import java.sql.DriverManager

object Runner extends App{
  val _ = classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://0.0.0.0:42895/product" //TODO: How do we do this??
  val port = 16000
  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

  val extensionsImpl = new Extensions {}

  val api = new API(extensionsImpl, conn).createApi()
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    DefaultRunner.runApi(api, port)
  }
}
