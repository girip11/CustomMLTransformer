name := "SparkCustomMLTransformer"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "2.4.4",
  "org.apache.spark" % "spark-sql_2.12" % "2.4.4",
  "org.apache.spark" % "spark-mllib_2.12" % "2.4.4",
  "org.scalacheck" % "scalacheck_2.12" % "1.14.3",
  // No need to add scala version to the scalatest artifact
  // The scala version is automatically appended
  "org.scalatest" %% "scalatest" % "3.1.1"
)