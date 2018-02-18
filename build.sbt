name := "KafkaSpark"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.3"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.6"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.3"