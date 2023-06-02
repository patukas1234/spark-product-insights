name := "spark_product_insights"

version := "0.1"

scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .enablePlugins(SbtPlugin)

// lazy val root = (project in file("."))
//  .settings(
//    name := "spark-product-insights",
//    idePackagePrefix := Some("ds.insights")
//  )


// cleanupCommands in console := "spark.stop()"
// cleanupCommands :=  "spark.stop()"
// scalacOptions ++= Seq("-unchecked", "-deprecation")

val sparkVersion = "3.4.0"

// resolvers += Resolver.sonatypeRepo("releases")
libraryDependencies += "org.apache.spark" %% "spark-core"  % sparkVersion  //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"   % sparkVersion  //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-yarn"  % sparkVersion  //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion  // % "provided"

libraryDependencies += "com.typesafe" % "config" % "1.4.2"
Test / logBuffered := false


/*assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
} */