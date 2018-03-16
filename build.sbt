name := "cassandra-assignment-01"

version := "1.0"

scalaVersion := "2.12.1"
libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.4.0",
  "com.typesafe"               %  "config"           % "1.3.1"
)

        