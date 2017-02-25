
name := "SparkExamples"
version := "1.0"
scalaVersion := "2.11.8"
val sparkVersion = "2.1.0"

// Add a single dependency
libraryDependencies += "junit" % "junit" % "4.8" % "test"
libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
                            "org.slf4j" % "slf4j-simple" % "1.7.5",
                            "org.clapper" %% "grizzled-slf4j" % "1.0.2")
libraryDependencies += "org.apache.spark"%%"spark-core"   % sparkVersion 
libraryDependencies += "org.apache.spark"%%"spark-streaming"   % sparkVersion 
libraryDependencies += "org.apache.spark"%%"spark-mllib"   % sparkVersion 
libraryDependencies += "org.apache.spark"%%"spark-streaming-flume-sink" % sparkVersion     
libraryDependencies += "org.apache.spark"%%"spark-streaming-kafka-0-10" % sparkVersion     

libraryDependencies += "org.apache.spark"%%"spark-sql"   % sparkVersion 
libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.0.0"

resolvers += "MavenRepository" at "https://mvnrepository.com/"

// Assembly settings
mainClass in Global := Some("SimpleReadMongoDataFile")

jarName in assembly := "spark-examples.jar"

// http://stackoverflow.com/questions/25144484/sbt-assembly-deduplication-found-error
assemblyMergeStrategy in assembly := {
  case PathList("javax", "mail", xs @ _*)         => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*)         => MergeStrategy.first
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList("org", "slf4j", xs @ _*)         => MergeStrategy.first
  case PathList("org", "apache", xs @ _*)         => MergeStrategy.first
  case PathList("org", "aopalliance", xs @ _*)         => MergeStrategy.first
  case PathList("javax", "mail", xs @ _*)         => MergeStrategy.first
  case PathList("org", "glassfish", xs @ _*)         => MergeStrategy.first
  case PathList("org", "apache", "commons-beanutils")         => MergeStrategy.first
  case PathList("org", "codehaus", xs @ _*)         => MergeStrategy.first
  case PathList("commons-beanutils", "commons-beanutils", "1.7.0") => MergeStrategy.discard
  case "overview.html" =>  MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


