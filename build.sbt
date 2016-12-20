/* build.sbt */
name := "hw6"
version := "0.1"
scalaVersion := "2.11.6"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.4.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.4.1"