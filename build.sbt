import AssemblyKeys._ 

name := "CassandraBenchmark"

version := "1.0"

organization := "pmarques.eu"

version := "1.0.0"

scalaVersion := "2.10.2"

scalacOptions ++= Seq("-deprecation", "-feature")

//---------------------------------------------------------------------------------------

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

libraryDependencies += "junit" % "junit" % "4.10" % "test"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "1.0.2"

libraryDependencies += "commons-codec" % "commons-codec" % "1.8"

//---------------------------------------------------------------------------------------

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
    case x => old(x)
  }
}

jarName in assembly := "CassandraBenchmark.jar"
