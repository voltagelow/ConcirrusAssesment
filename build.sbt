name := "CsvToParquet"
version := "0.1"
scalaVersion := "2.12.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.0" %"provided",
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.5"
