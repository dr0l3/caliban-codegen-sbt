sys.props.get("plugin.version") match {
  case Some(x) => addSbtPlugin("droletours" % "caliban-postgres-sbt" % x)
  case _ => sys.error("""|The system property 'plugin.version' is not defined.
                         |Specify this property using the scriptedLaunchOpts -D.""".stripMargin)
}

//addSbtPlugin("droletours" % "caliban-postgres-sbt" % "0.1.0-SNAPSHOT" changing())
//addSbtPlugin("com.tapad" % "sbt-docker-compose" % "1.0.35")