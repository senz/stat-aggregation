name := "stat-aggregator"

version := "1.1"

scalaVersion := "2.11.8"

lazy val akkaVersion = "2.4.2"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.2"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies += "com.google.jimfs" % "jimfs" % "1.1" % "test"

mainClass in Compile := Some("Main")