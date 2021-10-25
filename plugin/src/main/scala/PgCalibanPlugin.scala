import sbt.AutoPlugin
import sbt._
import Keys._
import codegen.PostgresSniffer.{connToTables, docToFederatedInstances, docToObjectExtension, parseDocument, parseExtensions, tablesToTableAPI, to2CalibanBoilerPlate}

import java.sql.DriverManager
import codegen._
import zio.{Schedule, ZIO, duration}

import java.util.concurrent.TimeUnit

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
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n == 12 => List(
          "com.github.ghostdogpr" %% "caliban" % "1.1.1+59-f6079748+20211017-1605-SNAPSHOT",
//          "com.github.ghostdogpr" %% "caliban-federation" % "1.1.1+59-f6079748+20211017-1605-SNAPSHOT",
        )
        case Some((2, n)) if n == 13 => List(
          "com.github.ghostdogpr" %% "caliban" % "1.1.1+59-f6079748+20211017-1606-SNAPSHOT",
//          "com.github.ghostdogpr" %% "caliban-federation" % "1.1.1+59-f6079748+20211017-1606-SNAPSHOT",
        )
        case _ => Nil
      }
    },
    pgCalibanExtensionFileLocation := file("./src/main/resources/extensions.graphql"),
    pgCalibanPostgresConnectionUrl := "jdbc:postgresql://0.0.0.0:5438/product",
    pgCalibanPostgresUser := "postgres",
    pgCalibanPostgresPassword := "postgres",
    pgCalibanGenerate := {
      Class.forName("org.postgresql.Driver")
      val _ = classOf[org.postgresql.Driver]
      val con_str = pgCalibanPostgresConnectionUrl.value
      DriverManager.setLoginTimeout(5)
      println(s"STARTING CONNECTION TO ${con_str}")
      val conn = zio.Runtime.default.unsafeRun(ZIO(DriverManager.getConnection(con_str, pgCalibanPostgresUser.value, pgCalibanPostgresPassword.value)).timeout(duration.Duration(1, TimeUnit.SECONDS)).retry(Schedule.exponential(duration.Duration(1, TimeUnit.SECONDS),2).upTo(duration.Duration(30, TimeUnit.SECONDS)))).get
      println("CONNECTION ESTABLISHED")

      val tables = connToTables(conn)

      println(pgCalibanExtensionFileLocation.value.toURI)
      println(pgCalibanExtensionFileLocation.value.exists())
      val (extensions, federatedInstances) = if(pgCalibanExtensionFileLocation.value.exists()) {
        val omg = new File(pgCalibanExtensionFileLocation.value.toURI)

        println("PAST OMG")

        val extensionDoc = parseDocument(parseExtensions(omg))
        println("PARSED STUFF")
        val federatedIstances = docToFederatedInstances(extensionDoc)
        (docToObjectExtension(extensionDoc), federatedIstances)
      } else {
        (Nil, Nil)
      }

      extensions.foreach(println)
      federatedInstances.foreach(println)

      println("MTH")

      val outputDir = (Compile / sourceManaged).value / "generated"
      outputDir.mkdirs()

      println("CREATED DIR")

      val outputFile = outputDir / "generated.scala"
      println("CREATED FILE")
      outputDir.createNewFile()

      println("ABOUT TO WRI")

      val generatedBoilerPlate = to2CalibanBoilerPlate(tablesToTableAPI(tables, extensions), extensions, tables, federatedInstances)

      IO.write(outputFile,  "package generated" ++ generatedBoilerPlate)

      println("WROTE TO FILE")
      outputFile
    }
  )

}
