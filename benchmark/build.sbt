import scala.sys.process._
import CustomTasks._
//
// spanner-spark-tests
//
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.15"

val sparkSqlVersions = Map(
  "3.1" -> "3.1.3",
  "3.2" -> "3.2.4",
  "3.3" -> "3.3.2"
)
val sparkVersion = sys.props.get("spark.version").getOrElse("3.3")

val sparkSqlVersion = sparkSqlVersions.getOrElse(sparkVersion, {
  sys.error(s"Unsupported spark.version: $sparkVersion. Supported versions are: ${sparkSqlVersions.keys.mkString(", ")}")
})

lazy val root = (project in file("."))
  .settings(
    name := "spanner-spark-benchmark",
    resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
    libraryDependencies ++= Seq(
      "com.google.cloud.spark.spanner" % s"spark-$sparkVersion-spanner" % "0.0.1-SNAPSHOT",
      "org.apache.spark" %% "spark-sql" % sparkSqlVersion % "provided",
      "com.typesafe.play" %% "play-json" % "2.9.2"
    ),
    Test / parallelExecution := false,
    // sbt-assembly settings
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.common.**" -> "com.google.cloud.spark.spanner.shaded.com.google.common.@1").inAll,
      ShadeRule.rename("com.google.protobuf.**" -> "com.google.cloud.spark.spanner.shaded.com.google.protobuf.@1").inAll
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) if xs.exists(_.endsWith(".SF")) || xs.exists(_.endsWith(".DSA")) || xs.exists(_.endsWith(".RSA")) =>
        MergeStrategy.discard
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat
      case "reference.conf" =>
        MergeStrategy.concat
      case _ =>
        MergeStrategy.first
    },

    Compile / mainClass := Some("com.google.cloud.spark.spanner.SparkSpannerWriteBenchmark")
  )
  .settings(CustomTasks.customTaskSettings)



