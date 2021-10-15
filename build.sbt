name := "caliban-codegen-sbt"

version := "0.1"

scalaVersion := "2.13.6"

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.24"
// https://mvnrepository.com/artifact/com.github.ghostdogpr/caliban
libraryDependencies += "com.github.ghostdogpr" %% "caliban" % "1.1.1+59-f6079748+20211011-1215-SNAPSHOT"
libraryDependencies += "com.github.ghostdogpr" %% "caliban-federation" % "1.1.1+59-f6079748+20211011-1215-SNAPSHOT"
libraryDependencies += "com.github.ghostdogpr" %% "caliban-http4s"     % "1.1.1" // routes for http4s
libraryDependencies += "com.github.ghostdogpr" %% "caliban-cats"       % "1.1.1" // interop with cats effect


// https://mvnrepository.com/artifact/io.circe/circe-core
libraryDependencies += "io.circe" %% "circe-core" % "0.14.1"
// https://mvnrepository.com/artifact/io.circe/circe-parser
libraryDependencies += "io.circe" %% "circe-parser" % "0.14.1"
libraryDependencies += "io.circe" %% "circe-optics" % "0.14.1"

libraryDependencies += "dev.zio" %% "zio-interop-cats" % "3.1.1.0"