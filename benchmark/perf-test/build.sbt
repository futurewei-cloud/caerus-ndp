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
name := "perf-test"
 
version := "1.0"
 
scalaVersion := "2.12.10"
val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
  "org.scalatest" % "scalatest_2.12" % "3.2.2",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.github.scopt" %% "scopt" % "4.0.0-RC2",
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

// scalastyle >= 0.9.0
compileScalastyle := scalastyle.in(Compile).toTask("").value

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

// Create a default Scala style task to run with tests
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

// scalastyle >= 0.9.0
testScalastyle := scalastyle.in(Test).toTask("").value

(test in Test) := ((test in Test) dependsOn testScalastyle).value