ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "3.8.4"

val PekkoVersion = "1.6.0"
ThisBuild / libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-actor-typed" % PekkoVersion,
  "org.apache.pekko" %% "pekko-cluster-typed" % PekkoVersion,
  "org.apache.pekko" %% "pekko-cluster-sharding-typed" % PekkoVersion,
  "org.apache.pekko" %% "pekko-serialization-jackson" % PekkoVersion,
  "ch.qos.logback" % "logback-classic" % "1.5.34"
)

Compile / scalaSource := baseDirectory.value

lazy val root = (project in file("."))
  .settings(
    name := "distributedSmartHomeAlarmSystem"
  )
