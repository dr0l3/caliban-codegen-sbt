

version := "0.2"



lazy val lib = project.in(file("lib")).settings(
  scalaVersion := "2.12.12",
  organizationName := "droletours",
  name := "caliban-postgres-lib",
  libraryDependencies ++= {
    Seq(
      "com.github.ghostdogpr" %% "caliban" % "1.2.0",
      "com.github.ghostdogpr" %% "caliban-federation" % "1.2.0"
    )
  },
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n == 12 => Seq("-Ypartial-unification")
      case _ => Seq()
    }
  },
  libraryDependencies += "org.postgresql" % "postgresql" % "42.2.24",
  libraryDependencies += "com.github.ghostdogpr" %% "caliban-http4s"     % "1.2.0", // routes for http4s
  libraryDependencies += "com.github.ghostdogpr" %% "caliban-cats"       % "1.2.0", // interop with cats effect
  // https://mvnrepository.com/artifact/io.circe/circe-core
  libraryDependencies += "io.circe" %% "circe-core" % "0.14.1",
  // https://mvnrepository.com/artifact/io.circe/circe-parser
  libraryDependencies += "io.circe" %% "circe-parser" % "0.14.1",
  libraryDependencies += "io.circe" %% "circe-optics" % "0.14.1",
  libraryDependencies += "io.circe" %% "circe-generic" % "0.14.1",
  libraryDependencies += "dev.zio" %% "zio-interop-cats" % "3.1.1.0",
  libraryDependencies ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2,n)) if n == 12 => Seq(compilerPlugin(
        "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
      ))
      case _ => Seq()
    }
  },
  libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.5.0",
  crossScalaVersions := List("2.12.12", "2.13.6")
)

lazy val playground = project.in(file("playground"))
  .settings(
    organizationName := "droletours",
    name := "caliban-postgres-playground",
    publish / skip := true,
    scalacOptions ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n == 12 => Seq("-Ypartial-unification")
        case _ => Seq()
      }
    },
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2,n)) if n == 12 => Seq(compilerPlugin(
          "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
        ))
        case _ => Seq()
      }
    },
  ).dependsOn(lib)

lazy val plugin = project.in(file("plugin"))
  .enablePlugins(SbtPlugin)
  .settings(
    organization := "droletours",
    name := "caliban-postgres-sbt",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.2.24",
//    libraryDependencies ++= {
//      CrossVersion.partialVersion(scalaVersion.value) match {
//        case Some((2, n)) if n == 12 => List(
//          "com.github.ghostdogpr" %% "caliban" % "1.1.1+59-f6079748+20211017-1605-SNAPSHOT",
//          "com.github.ghostdogpr" %% "caliban-federation" % "1.1.1+59-f6079748+20211017-1605-SNAPSHOT",
//        )
//        case Some((2, n)) if n == 13 => List(
//          "com.github.ghostdogpr" %% "caliban" % "1.1.1+59-f6079748+20211017-1606-SNAPSHOT",
//          "com.github.ghostdogpr" %% "caliban-federation" % "1.1.1+59-f6079748+20211017-1606-SNAPSHOT",
//        )
//        case _ => Nil
//      }
//    },
//    scriptedDependencies :={
//      (lib / publishLocal).value
//    }
  )
  .dependsOn(lib)