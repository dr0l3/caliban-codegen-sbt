import generated.Definitions.{API, Extensions,productcustomExtensionArg}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Connection
import java.io.File
import zio.ZIO
import caliban.execution.Field

class Test extends AnyFunSuite with Matchers {

  val _ = classOf[org.postgresql.Driver]

  val extensionsImpl = new Extensions {
    def productcustom(productcustomExtensionArg: productcustomExtensionArg): ZIO[Any,Throwable, Option[String]] = ZIO(Option(s"custom ${productcustomExtensionArg.upc}"))
  }

  TestUtil.createTests(new File("src/test/fixtures"), (conn: Connection) => new API(extensionsImpl,conn).createApi()).foreach { case (testRun, testName) =>
    test(testName) {
      zio.Runtime.default.unsafeRun(testRun)
    }
  }
}
