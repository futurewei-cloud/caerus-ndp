name := "ndp-datasource"

organization := ""
version := "0.1.0"
scalaVersion := "2.12.10"

libraryDependencies ++= {
  val sparkVersion = "3.1.1"
  val log4jVersion = "1.2.7"
  val slf4jVersion = "1.7.30"
  Seq(
  "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M3",
  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "org.apache.commons" % "commons-lang3" % "3.10" % "test",
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",

  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-client" % "3.2.2" % "provided",

  "org.slf4j" % "slf4j-api" % slf4jVersion % "provided",
  "log4j" % "log4j" % log4jVersion)
}

test in assembly := {}
assemblyJarName in assembly := "ndp-datasource-with-dependencies.1.0.jar"
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case "module-info.class"                           => MergeStrategy.discard
  case "scala-xml.properties"                        => MergeStrategy.discard
  case "public-suffix-list.txt"                      => MergeStrategy.discard
  case PathList("scala", "xml", xs @ _*)             => MergeStrategy.discard
  case PathList("io", "netty", xs @ _*)             => MergeStrategy.discard
  case PathList("javax", "xml", xs @ _*)             => MergeStrategy.discard
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}