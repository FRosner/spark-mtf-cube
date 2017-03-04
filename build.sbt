organization  := "de.frosner"

version       := "0.1.0-SNAPSHOT"

name          := "spark-mtf-cube"

scalaVersion  := "2.11.8"

sparkVersion := "2.1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided"

resolvers += "Sonatype Public" at "https://oss.sonatype.org/content/groups/public/"

libraryDependencies += "org.scodec" %% "scodec-core" % "1.10.3" // see http://scodec.org/releases/ if using cross version build

fork := true

javaOptions += "-Xmx2G"
