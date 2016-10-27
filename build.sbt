// Set the project name to the string 'My Project'
name := "SparkExamples"

// The := method used in Name and Version is one of two fundamental methods.
// The other method is <<=
// All other initialization methods are implemented in terms of these.
version := "1.0"

scalaVersion := "2.10.5"

assemblyJarName in assembly := "sparkexamples.jar"


// Add a single dependency
libraryDependencies += "junit" % "junit" % "4.8" % "test"
libraryDependencies += "org.mockito" % "mockito-core" % "1.9.5"
libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
                            "org.slf4j" % "slf4j-simple" % "1.7.5",
                            "org.clapper" %% "grizzled-slf4j" % "1.0.2")
libraryDependencies += "org.powermock" % "powermock-mockito-release-full" % "1.5.4" % "test"
libraryDependencies += "org.apache.spark" %% "spark-core"   % "1.6.1" 
libraryDependencies += "org.apache.spark" %% "spark-streaming"   % "1.6.1" 
libraryDependencies += "org.apache.spark" %% "spark-mllib"   % "1.6.1"  
libraryDependencies += "org.apache.spark" %% "spark-streaming-flume"   % "1.3.0"  
libraryDependencies += "org.apache.spark" %% "spark-sql"   % "1.6.1" 
libraryDependencies +=  "com.databricks"  %% "spark-csv" % "1.5.0" 


resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"



