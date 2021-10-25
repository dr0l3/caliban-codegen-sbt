import caliban.ResponseValue
import caliban.Value.{IntValue, StringValue}
import generated.{API, Extensions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.DriverManager

class Test extends AnyFlatSpec with Matchers {

  "The api" should "handle get request" in {
    val _ = classOf[org.postgresql.Driver]
    val con_str = System.getenv("PG_URL")
    val conn = DriverManager.getConnection(con_str, "postgres", "postgres")

    val extensionsImpl = new Extensions {}

    val api = new API(extensionsImpl, conn).createApi()

    val query =
      s"""{
         | get_product_by_id(upc: "1") {
         |  upc
         |  name
         |  price
         |  weight
         | }
         |}""".stripMargin

    zio.Runtime.default.unsafeRun {
      for {
        interpreter <- api.interpreter
        res <- interpreter.execute(query)
        expectedResult = ResponseValue.ObjectValue(
          List(
            "get_product_by_id" -> ResponseValue.ObjectValue(
              List(
                "weight" -> IntValue(100),
                "price" -> IntValue(899),
                "name" -> StringValue("Table"),
                "upc" -> StringValue("1"),
              )
            )
          )
        )
      } yield ResponseValue.circeEncoder.apply(res.data) shouldBe ResponseValue.circeEncoder.apply(expectedResult)
    }
  }
}
