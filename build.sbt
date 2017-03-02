organization  := "de.frosner"

version       := "0.1.0-SNAPSHOT"

name          := "custom-spark-datasource"

scalaVersion  := "2.11.8"

sparkVersion := "2.1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided"

resolvers += "Sonatype Public" at "https://oss.sonatype.org/content/groups/public/"

libraryDependencies += "org.scodec" %% "scodec-bits" % "1.1.2"

fork := true

javaOptions += "-Xmx2G"
