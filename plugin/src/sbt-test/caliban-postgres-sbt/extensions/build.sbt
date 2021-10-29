
name := "extensions"

version := "0.1"

scalaVersion := "2.13.6"

enablePlugins(PgCalibanPlugin)
enablePlugins(PgCalibanTestPlugin)

libraryDependencies += "caliban-postgres-lib" %% "caliban-postgres-lib" % sys.props("plugin.version") changing()
libraryDependencies += "droletours" %% "test-util" % sys.props("plugin.version") % Test changing()

pgCalibanExtensionFileLocation := file(".")/ "src"/"main"/"resources"/"extensions.graphql"
pgCalibanPostgresConnectionUrl := {
  s"jdbc:postgresql://0.0.0.0:${pgCalibanTestPort.value}/product"
}


