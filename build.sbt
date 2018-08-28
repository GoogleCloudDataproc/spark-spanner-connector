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

name         := "cloud-spanner-spark-connector"
organization := "com.google.cloud"
version      := "0.1.0-alpha-SNAPSHOT"

scalaVersion := "2.11.12"

libraryDependencies += "com.google.cloud" % "google-cloud-spanner" % "0.58.0-beta"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % Provided

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
