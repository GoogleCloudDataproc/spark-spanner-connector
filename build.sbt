// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

name         := "spark-cloud-spanner"
organization := "com.google.cloud.spark"
version      := "0.1.0-alpha-SNAPSHOT"

scalaVersion := "2.12.8"

libraryDependencies += "com.google.cloud" % "google-cloud-spanner" % "1.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % Provided

val scalatestVer = "3.2.0-SNAP10"
libraryDependencies += "org.scalactic" %% "scalactic" % scalatestVer
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVer % Test
// scalacheck to fix an issue with scalatest
// (Test / executeTests) java.lang.NoClassDefFoundError: org/scalacheck/Test$TestCallback
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % Test

// https://github.com/sbt/sbt-assembly#shading
// https://cloud.google.com/blog/products/data-analytics/managing-java-dependencies-apache-spark-applications-cloud-dataproc
test in assembly := {}
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename(
    "com.google.common.**" -> "repackaged.com.google.common.@1",
    "com.google.protobuf.**" -> "repackaged.com.google.protobuf.@1"
  ).inAll
)
