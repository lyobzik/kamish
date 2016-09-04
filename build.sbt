name := "kamish"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.typesafe" % "config" % "1.3.0",
  "org.apache.kafka" % "kafka_2.10" % "0.10.0.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0"
)