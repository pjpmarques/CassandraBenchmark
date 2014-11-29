name := "CassandraBenchmark"

version := "1.0"

organization := "pmarques.eu"

version := "1.0.0"

scalaVersion := "2.11.4"

scalacOptions ++= Seq("-deprecation", "-feature")


//---------------------------------------------------------------------------------------

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test"

libraryDependencies += "junit" % "junit" % "4.10" % "test"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.2"

libraryDependencies += "commons-cli" % "commons-cli" % "1.2"

//---------------------------------------------------------------------------------------

// assemblySettings
// 

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
	{
		case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
		case x => old(x)
	}
}

jarName in assembly := "CassandraBenchmark.jar"
