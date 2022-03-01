import sbt.AutoPlugin
import sbt._
import Keys._
import codegen.PostgresSniffer.{connToTables2, docToFederatedInstances, docToObjectExtension, docToObjectExtensionV2, extractTable, parseDocument, parseExtensions, results, toTablesV2}
import codegen.Version2.{ZIOEffectWrapper, ZQueryEffectWrapper, toCalibanAPI}

import java.sql.{Connection, DriverManager}
import codegen._
import zio.{Managed, Schedule, URIO, ZIO, ZManaged, duration}

import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import scala.sys.process.Process
import com.impossibl.postgres.api.jdbc.PGConnection
import com.impossibl.postgres.api.jdbc.PGNotificationListener
import com.impossibl.postgres.jdbc.PGDataSource
import com.impossibl.postgres.jdbc.PGDriver

import java.lang.Throwable

object Util {
  def randomPort(): Int = {
    val socket = new ServerSocket(0)
    socket.setReuseAddress(true)
    val port = socket.getLocalPort
    try {
      socket.close()
    } finally {
      socket.close()
    }
    port
  }

  def logic(pgCalibanExtensionFileLocation: File, conn: Connection, outputDir: File) = {
    implicit val c = conn
    val (extensions, federatedInstances, v2Extension) =
      if (pgCalibanExtensionFileLocation.exists()) {
        val omg = new File(pgCalibanExtensionFileLocation.toURI)

        println("PAST OMG")

        val extensionDoc = parseDocument(parseExtensions(omg))
        println("PARSED STUFF")
        val federatedIstances = docToFederatedInstances(extensionDoc)
        val v2extensions = docToObjectExtensionV2(extensionDoc)
        (docToObjectExtension(extensionDoc), federatedIstances, v2extensions)
      } else {
        (Nil, Nil, Nil)
      }

    extensions.foreach(println)
    federatedInstances.foreach(println)

    println("MTH")

    outputDir.mkdirs()

    println("CREATED DIR")

    val outputFile = outputDir / "generated.scala"
    println("CREATED FILE")
    outputDir.createNewFile()



    println("ABOUT TO WRI")
    try {
      println("INSIDE TRY")

      val tables2 = connToTables2(v2Extension)(conn)
      println("HEHEHE")
      println("TABLES")
      val boilerPlate2 = toCalibanAPI(tables2, v2Extension).render(ZIOEffectWrapper)

      println("BOILERPLATE DONE")
      IO.write(outputFile, "package generated" ++ boilerPlate2)

      println("WROTE TO FILE")
    } catch {
      case t: Throwable =>
        println("WUT")
        t.printStackTrace()
    } finally {
      println("WOOOOTO!")
    }

  }
}


object PgCalibanTestPlugin extends AutoPlugin {
  object autoImport {
    lazy val pgCalibanTestPort =
      settingKey[Int]("The port to use for the test run")
    lazy val pgCalibanStartDatabase = taskKey[Unit]("Start a test database")
    lazy val pgCalibanShutDownDatabase = taskKey[Unit]("Shut down the database")
  }

  import autoImport._

  override lazy val projectSettings = Seq(
    pgCalibanTestPort := Util.randomPort(),
    pgCalibanStartDatabase := {
      val extraEnvs = List(
        "POSTGRES_PORT" -> pgCalibanTestPort.value.toString,
        "TEST_NAME" -> name.value
      )
      Process(
        s"""docker-compose -f ./docker/docker-compose.yml down -v""",
        new java.io.File("."),
        extraEnv = extraEnvs: _*
      ).!
      Process(
        s"""docker-compose -f ./docker/docker-compose.yml build --no-cache""",
        new java.io.File("."),
        extraEnv = extraEnvs: _*
      ).!
      Process(
        s"""docker-compose -f ./docker/docker-compose.yml up -d""",
        new java.io.File("."),
        extraEnv = extraEnvs: _*
      ).!
    },
    pgCalibanShutDownDatabase := {
      val extraEnvs = List(
        "POSTGRES_PORT" -> pgCalibanTestPort.value.toString,
        "TEST_NAME" -> name.value
      )
      Process(
        s"""docker-compose -f ./docker/docker-compose.yml down -v""",
        new java.io.File("."),
        extraEnv = extraEnvs: _*
      ).!
    },
    commands += Command.command("execute-sbt-test") { state =>
      "pgCalibanStartDatabase" :: "pgCalibanGenerate" :: "compile" :: "test" :: "pgCalibanShutDownDatabase" :: state
    }
  )
}

object PgCalibanPlugin extends AutoPlugin {
  object autoImport {
    lazy val pgCalibanExtensionFileLocation =
      settingKey[File]("File location of extensions file")
    lazy val pgCalibanPostgresConnectionUrl = settingKey[String](
      "The connection url to postgres format is jdbc:postgresql://0.0.0.0:5438/product"
    )
    lazy val pgCalibanPostgresUser =
      settingKey[String]("The user to connect with")
    lazy val pgCalibanPostgresPassword =
      settingKey[String]("The password to use")
    lazy val pgCalibanGenerate = taskKey[Seq[File]]("generate the code")
    lazy val pgCalibanGenerateContinous = taskKey[Seq[File]]("generate code continously")
  }

  import autoImport._

  override lazy val projectSettings = Seq(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n == 12 =>
          List(
            "com.github.ghostdogpr" %% "caliban" % "1.1.1+59-f6079748+20211017-1605-SNAPSHOT"
          )
        case Some((2, n)) if n == 13 =>
          List(
            "com.github.ghostdogpr" %% "caliban" % "1.1.1+59-f6079748+20211017-1606-SNAPSHOT"
          )
        case _ => Nil
      }
    },
    pgCalibanExtensionFileLocation := file(
      "./src/main/resources/extensions.graphql"
    ),
    pgCalibanPostgresConnectionUrl := "jdbc:postgresql://0.0.0.0:5438/product",
    pgCalibanPostgresUser := "postgres",
    pgCalibanPostgresPassword := "postgres",
    pgCalibanGenerate := {
      Class.forName("org.postgresql.Driver")
      val _ = classOf[org.postgresql.Driver]
      val con_str = pgCalibanPostgresConnectionUrl.value
      DriverManager.setLoginTimeout(5)
      println(s"STARTING CONNECTION TO ${con_str}")
      val conn = zio.Runtime.default
        .unsafeRun(
          ZIO(
            DriverManager.getConnection(
              con_str,
              pgCalibanPostgresUser.value,
              pgCalibanPostgresPassword.value
            )
          ).timeout(duration.Duration(1, TimeUnit.SECONDS))
            .retry(
              Schedule
                .exponential(duration.Duration(1, TimeUnit.SECONDS), 2)
                .upTo(duration.Duration(30, TimeUnit.SECONDS))
            )
        )
        .get
      println("CONNECTION ESTABLISHED")

      println(pgCalibanExtensionFileLocation.value.toURI)
      println(pgCalibanExtensionFileLocation.value.exists())
      val (extensions, federatedInstances, v2Extension) =
        if (pgCalibanExtensionFileLocation.value.exists()) {
          val omg = new File(pgCalibanExtensionFileLocation.value.toURI)

          println("PAST OMG")

          val extensionDoc = parseDocument(parseExtensions(omg))
          println("PARSED STUFF")
          val federatedIstances = docToFederatedInstances(extensionDoc)
          val v2extensions = docToObjectExtensionV2(extensionDoc)
          (docToObjectExtension(extensionDoc), federatedIstances, v2extensions)
        } else {
          (Nil, Nil, Nil)
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
      val tables2 = connToTables2(v2Extension)(conn)
      val boilerPlate2 =
        toCalibanAPI(tables2, v2Extension).render(ZIOEffectWrapper)

      IO.write(outputFile, "package generated" ++ boilerPlate2)

      println("WROTE TO FILE")
      List(outputFile)
    },
    pgCalibanGenerateContinous := {
      Class.forName("org.postgresql.Driver")
      val _ = classOf[com.impossibl.postgres.jdbc.PGDriver]
      val con_str = pgCalibanPostgresConnectionUrl.value.replaceAllLiterally("postgresql", "pgsql")

      val dataSource = new PGDataSource()
      dataSource.setDatabaseUrl(con_str)
      dataSource.setUser(pgCalibanPostgresUser.value)
      dataSource.setPassword(pgCalibanPostgresPassword.value)

      val con2 = DriverManager.getConnection(
        con_str,
        pgCalibanPostgresUser.value,
        pgCalibanPostgresPassword.value
      )


      //https://github.com/graphile/graphile-engine/blob/master/packages/graphile-build-pg/res/watch-fixtures.sql
      //https://www.postgresql.org/docs/current/event-trigger-definition.html


      println(s"STARTING CONNECTION TO ${con_str}")
      zio.Runtime.default
        .unsafeRun(
          ZManaged.makeEffect(dataSource.getConnection().unwrap(classOf[PGConnection]))(v => v.close())
            .use { conn =>
              conn.addNotificationListener(new Listen(pgCalibanExtensionFileLocation.value, con2, (Compile / sourceManaged).value / "generated"))
              for {
                _ <- ZIO(conn.createStatement().executeUpdate("LISTEN postgraphile_watch"))
                _ <- ZIO.never
              } yield ()
            }
        )
      println("CONNECTION ESTABLISHED")

//      ((ZIO {
//        val conn = dataSource.getConnection().unwrap(classOf[PGConnection])
//        println("SETTING UP CONNECTION")
//        conn.addNotificationListener(new Listen(pgCalibanExtensionFileLocation.value, conn,(Compile / sourceManaged).value / "generated"))
//        conn.createStatement().executeUpdate("LISTEN postgraphile_watch")
//        println("EXECUTED LISTEN")
//        println("ZOMG NEW")
//      }.toManaged(v => ZIO.succeed()).useForever
//
//
//        ).timeout(duration.Duration(1, TimeUnit.SECONDS))
//        .retry(
//          Schedule
//            .exponential(duration.Duration(1, TimeUnit.SECONDS), 2)
//            .upTo(duration.Duration(30, TimeUnit.SECONDS))
//        )).flatMap(_ => ZIO.infinity)

      println(pgCalibanExtensionFileLocation.value.toURI)
      println(pgCalibanExtensionFileLocation.value.exists())



      List()
    },
    Compile / sourceGenerators += pgCalibanGenerate
  )

}



class Listen(pgCalibanExtensionFileLocation: File, conn: Connection,outputDir: File) extends PGNotificationListener {
  override def notification(processId: Int, channelName: String, payload: String): Unit = {
    println(s"$processId -> $channelName: $payload")
    Util.logic(pgCalibanExtensionFileLocation, conn, outputDir)
  }
}
