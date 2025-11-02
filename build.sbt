name := "eurowings-scala-challenge"
version := "0.1.0"
scalaVersion := "2.12.18"
ThisBuild / organization := "com.eurowingsholidays"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "com.typesafe"      % "config"     % "1.4.3",
  "org.scalatest"    %% "scalatest"  % "3.2.19" % Test
)

Compile / run / fork := true
Compile / run / javaOptions ++= Seq(
  "-Dconfig.file=conf/application.conf",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "-Dspark.master=local[*]"
)

Compile / mainClass := Some("com.eurowingsholidays.ingest.Main")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

// === SBT Assembly settings ===
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => xs match {
    case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
    case ("services" :: _)       => MergeStrategy.filterDistinctLines
    case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
      MergeStrategy.filterDistinctLines
    case _ => MergeStrategy.discard
  }

  // Handle duplicate protobuf / Jackson / Netty / Arrow / Log4j files
  case PathList("google", "protobuf", _ @ _*)           => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", "versions", _ @ _*)         => MergeStrategy.first
  case PathList("META-INF", "org", "apache", "logging", "log4j", _ @ _*) =>
    MergeStrategy.first
  case PathList("arrow-git.properties")                 => MergeStrategy.first
  case "module-info.class"                              => MergeStrategy.discard
  case _                                                => MergeStrategy.first
}
