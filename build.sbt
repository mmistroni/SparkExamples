// Set the project name to the string 'My Project'
name := "SparkExamples"

// The := method used in Name and Version is one of two fundamental methods.
// The other method is <<=
// All other initialization methods are implemented in terms of these.
version := "1.0"

scalaVersion := "2.11.7"

assemblyJarName in assembly := "sparkexamples.jar"

mainClass in assembly := Some("SparkLauncher")

// Add a single dependency
libraryDependencies += "junit" % "junit" % "4.8" % "test"
libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.5"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
libraryDependencies += "org.mockito" % "mockito-core" % "1.9.5"
libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
                            "org.slf4j" % "slf4j-simple" % "1.7.5",
                            "org.clapper" %% "grizzled-slf4j" % "1.0.2")
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
libraryDependencies += "org.powermock" % "powermock-mockito-release-full" % "1.5.4" % "test"
libraryDependencies += "org.apache.spark" %% "spark-core"   % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming"   % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib"   % "1.6.1"  % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-flume"   % "1.3.0"  % "provided"

resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"



