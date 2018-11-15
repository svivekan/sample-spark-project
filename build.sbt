name := "sample-spark"

version := "0.0.1"

scalaVersion := "2.11.11"

sparkVersion := "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.google.guava" % "guava" % "12.0"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_2.11-${sparkVersion.value}_${version.value}.jar"


