import sbt.AutoPlugin
import sbt._
import Keys._
import codegen.PostgresSniffer.{connToTables, docToObjectExtension, parseDocument, parseExtensions, tablesToTableAPI, to2CalibanBoilerPlate}

import java.sql.DriverManager
import codegen._

object PgCalibanPlugin extends AutoPlugin {
  object autoImport {
    lazy val pgCalibanExtensionFileLocation = settingKey[File]("File location of extensions file")
    lazy val pgCalibanPostgresConnectionUrl = settingKey[String]("The connection url to postgres format is jdbc:postgresql://0.0.0.0:5438/product")
    lazy val pgCalibanPostgresUser = settingKey[String]("The user to connect with")
    lazy val pgCalibanPostgresPassword = settingKey[String]("The password to use")
    lazy val pgCalibanGenerate = taskKey[File]("generate the code")
  }

  import autoImport._

  override lazy val projectSettings = Seq(
    pgCalibanExtensionFileLocation := file("src/main/resources/extensions.graphql"),
    pgCalibanPostgresConnectionUrl := "jdbc:postgresql://0.0.0.0:5438/product",
    pgCalibanPostgresUser := "postgres",
    pgCalibanPostgresPassword := "postgres",
    pgCalibanGenerate := {
      Class.forName("org.postgresql.Driver")
      val _ = classOf[org.postgresql.Driver]
      val con_str = pgCalibanPostgresConnectionUrl.value
      val conn = DriverManager.getConnection(con_str, pgCalibanPostgresUser.value, pgCalibanPostgresPassword.value)

      val tables = connToTables(conn)

      val extensions = if(pgCalibanExtensionFileLocation.value.exists()) {
        val omg = new File(pgCalibanExtensionFileLocation.value.toURI)

        println("PAST OMG")

        val extensionDoc = parseDocument(parseExtensions(omg))
        println("PARSED STUFF")
        docToObjectExtension(extensionDoc)
      } else {
        Nil
      }


      println("MTH")

      val outputDir = (sourceManaged in Compile).value / "generated"
      outputDir.mkdirs()

      println("CREATED DIR")

      val outputFile = outputDir / "generated.scala"
      println("CREATED FILE")
      outputDir.createNewFile()

      println("ABOUT TO WRI")

      val generatedBoilerPlate = to2CalibanBoilerPlate(tablesToTableAPI(tables), extensions, tables)

      IO.write(outputFile,  "package generated" ++ generatedBoilerPlate)

      println("WROTE TO FILE")
      outputFile
    }
  )

}
