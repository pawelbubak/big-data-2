name := "fact"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.4.4",
    "org.apache.spark" % "spark-sql_2.11" % "2.4.4"
)
