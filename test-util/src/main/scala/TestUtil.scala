import caliban.{GraphQL, InputValue, ResponseValue}
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import zio.ZIO

import java.sql.{Connection, DriverManager}
import java.io.File
import io.circe.{JsonObject, parser}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


object TestUtil {

  def readVariables(vars: String): Either[Throwable, Map[String, InputValue]] = {
    for {
      parsed <- parser.parse(vars)
      variables <- parsed
        .asObject
        .getOrElse(JsonObject.empty)
        .toMap
        .mapValues(json => InputValue.circeDecoder.decodeJson(json))
        .foldLeft[Either[Throwable, Map[String,InputValue]]](Right(Map.empty)) { case (acc, (k, v)) =>
          for {
            map <- acc
            value <- v.toTry.toEither
          } yield map + (k -> value)
        }
    } yield variables
  }

  def initializeDatabase(conn: Connection) = {
    val statement = conn.createStatement()
    new File("docker/init").listFiles().sortBy(_.getName).foreach{ initFile =>
      val sql = scala.reflect.io.File(initFile.getAbsolutePath).lines().filterNot(_.startsWith("\\")).toList.mkString("").split(";")
      sql.foreach{sqlLine =>
        statement.execute(sqlLine)
      }
    }
    statement.close()
  }


  def createTests(dir: File, apiFactory: Connection => GraphQL[Any]): Array[(ZIO[Any, Throwable, Unit], String)] = {
    dir.listFiles().filter(_.isDirectory).map{ testCaseDir =>

      val pg = EmbeddedPostgres.start()
      val url = pg.getJdbcUrl("postgres", "postgres")
      val conn = DriverManager.getConnection(url, "postgres", "postgres")
      initializeDatabase(conn)
      val api = apiFactory(conn)

      val testCaseName = testCaseDir.getName
      val runs = testCaseDir.listFiles().filter(_.isDirectory).sortBy(_.getName).map { testOp =>
        val query = scala.reflect.io.File(s"${testOp.getAbsolutePath}/query.txt").slurp()
        val variables = scala.reflect.io.File(s"${testOp.getAbsolutePath}/variables.json").slurp()
        val expectedResult = scala.reflect.io.File(s"${testOp.getAbsolutePath}/result.json").slurp()
        for {
          interpreter <- api.interpreter
          variablesAsInput <- ZIO.fromEither(readVariables(variables))
          res <- interpreter.execute(query, variables = variablesAsInput)
          parsedResult <- ZIO.fromEither(parser.parse(expectedResult))
        } yield ResponseValue.circeEncoder.apply(res.data) shouldBe parsedResult
      }

      (ZIO.collectAll_(runs), testCaseName)
    }
  }
}
