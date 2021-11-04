import generated.Definitions.{API, Extensions,productcustomExtensionArg}
import runner.DefaultRunner
import caliban.execution.Field

import zio._

import java.sql.DriverManager

object Runner extends App{
  val _ = classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://0.0.0.0:42895/product" //TODO: How do we do this??
  val port = 16000
  implicit val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

  val extensionsImpl = new Extensions {
    def productcustom(productcustomExtensionArg: productcustomExtensionArg): ZIO[Any,Throwable, Option[String]] = ZIO(Option(s"custom ${productcustomExtensionArg.upc}"))
  }

  val api = new API(extensionsImpl, conn).createApi()
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    DefaultRunner.runApi(api, port)
  }
}
