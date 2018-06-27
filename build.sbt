
name := "sphero-iot-infraction-detector"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.8",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.8" % Test,

  "com.typesafe.akka" %% "akka-http" % "10.0.11",

  "com.typesafe.akka" %% "akka-stream-kafka" % "0.21.1",

  "com.typesafe.akka" %% "akka-http-testkit" % "10.0.11" % Test,
  "org.java-websocket" % "Java-WebSocket" % "1.3.0" % Test,

  "org.scalatest" % "scalatest_2.12" % "3.0.4" % Test,

  "io.spray" %%  "spray-json" % "1.3.2"
)