name := "trail"
version := "1.0"
scalaVersion := "2.12.14"
val sparkVer = "3.1.3"

libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % "3.1.3",
    "org.apache.spark" %% "spark-mllib" % sparkVer,
    "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.3.3")
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.3" % "provided"
