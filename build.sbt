name := "structured-streaming-application"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.10.2.1",
  "com.typesafe.akka" %% "akka-actor" % "2.5.8"
)
