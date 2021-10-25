import java.net.ServerSocket
import sys.process._

name := "single-table"

version := "0.1"

scalaVersion := "2.13.6"

enablePlugins(PgCalibanPlugin)

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
val postgresPortNumber = settingKey[Int]("wahtever")

postgresPortNumber := randomPort()

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"


sys.props.get("plugin.version") match {
  case Some(x) => libraryDependencies += "caliban-postgres-lib" %% "caliban-postgres-lib" % x
  case _ => sys.error("""|The system property 'lib.version' is not defined.
                         |Specify this property using the scriptedLaunchOpts -D.""".stripMargin)
}

pgCalibanExtensionFileLocation := file(".")/ "src"/"main"/"resources"/"extensions.graphql"
pgCalibanPostgresConnectionUrl := {
  s"jdbc:postgresql://0.0.0.0:${postgresPortNumber.value}/product"
}
pgCalibanPostgresUser :=  "postgres"
pgCalibanPostgresPassword := "postgres"

val doStuff = taskKey[Unit]("do the stuff")
val killStuff =taskKey[Unit]("kil lstuff")
val configStuff = taskKey[Unit]("something")

doStuff := {
  Process(s"""docker-compose -f ./docker/docker-compose.yml build""",new java.io.File("."), extraEnv = "POSTGRES_PORT"-> postgresPortNumber.value.toString).!
  Process(s"""docker-compose -f ./docker/docker-compose.yml up -d""",new java.io.File("."), extraEnv = "POSTGRES_PORT"-> postgresPortNumber.value.toString).!
}

killStuff :=  {
  Process(s"""docker-compose -f ./docker/docker-compose.yml down -v""",new java.io.File("."), extraEnv = "POSTGRES_PORT"-> "0").!
}

Test / fork := true
envVars := Map("PG_URL" -> pgCalibanPostgresConnectionUrl.value)
Test / envVars := Map("PG_URL" -> pgCalibanPostgresConnectionUrl.value)

commands += Command.command("meh"){ state =>
  "doStuff":: "pgCalibanGenerate" :: "compile" :: "test" :: "killStuff" :: state
}
