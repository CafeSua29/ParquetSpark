name := "ParquetSpark"

version := "1.0"

scalaVersion := "2.12.18" // Compatible with Spark 3.5.2

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.2",
  "org.apache.spark" %% "spark-sql" % "3.5.2"
)

// Assembly plugin settings (do not modify if not using assembly-specific options)
assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}