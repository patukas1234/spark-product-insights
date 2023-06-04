name := "spark_product_insights"

version := "0.7.0-SNAPSHOT"

scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .enablePlugins(SbtPlugin)

val sparkVersion = "3.4.0"

// resolvers += Resolver.sonatypeRepo("releases")
libraryDependencies += "org.apache.spark" %% "spark-core"  % sparkVersion  //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"   % sparkVersion  //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-yarn"  % sparkVersion  //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion  // % "provided"

libraryDependencies += "com.typesafe" % "config" % "1.4.2"

Test / logBuffered := false

assembly / assemblyMergeStrategy := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}