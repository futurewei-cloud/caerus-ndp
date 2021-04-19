/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
name := "ndp-datasource-main"
organization in ThisBuild := ""
scalaVersion in ThisBuild := "2.12.10"
version in ThisBuild := "0.1.0"

lazy val root = project
  .in(file("."))
  .aggregate(
    datasource,
    examples)

lazy val datasource = project
  .in(file("datasource"))
  .settings(
    name := "ndp-datasource",
    libraryDependencies ++= commonDependencies,
  )
lazy val examples = project
  .in(file("examples"))
  .settings(
    name := "ndp-examples",
    libraryDependencies ++= commonDependencies,
  )

lazy val commonDependencies = {
  val sparkVersion = "3.1.1"
  val log4jVersion = "1.2.7"
  val slf4jVersion = "1.7.30"
  Seq(
  "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M3",
  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "org.apache.commons" % "commons-lang3" % "3.10" % "test",
  "org.scala-lang" % "scala-library" % "2.12.10" % "compile",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "test-sources",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "test-sources",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "test-sources",
  "org.apache.hadoop" % "hadoop-client" % "3.2.2" % "provided",

  "de.siegmar" % "fastcsv" % "2.0.0",
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
