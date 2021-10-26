import generated.{API, Extensions}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Connection
import java.io.File

class Test extends AnyFunSuite with Matchers {

  val _ = classOf[org.postgresql.Driver]

  val extensionsImpl = new Extensions {}

  TestUtil.createTests(new File("src/test/fixtures"), (conn: Connection) => new API(extensionsImpl,conn).createApi()).foreach { case (testRun, testName) =>
    test(testName) {
      zio.Runtime.default.unsafeRun(testRun)
    }
  }
}
