lazy val commonSettings = Seq(
  organization := "at.ac.univie.spark",
  version := "0.1.0",
  scalaVersion := "2.10.5"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "Spark"
  )

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.1.0",
  "org.apache.spark" % "spark-mllib_2.10" % "2.1.0"
)

/*
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.1.0" % provided,
  "org.apache.spark" % "spark-mllib_2.10" % "2.1.0" % provided
)
*/
