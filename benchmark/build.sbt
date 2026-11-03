import scala.sys.process._
import BenchmarkingTasks._

// spanner-spark-benchmark

ThisBuild / version := "0.1"

val sparkSqlVersions = Map(
  "3.1" -> "3.1.3",
  "3.2" -> "3.2.4",
  "3.3" -> "3.3.2",
  "3.5" -> "3.5.7",
  "4.0" -> "4.0.1",
  "4.1" -> "4.1.2"
)

val sparkScalaVersions = Map(
  "3.1" -> "2.12.15",
  "3.2" -> "2.12.15",
  "3.3" -> "2.12.15",
  "3.5" -> "2.12.15",
  "4.0" -> "2.13.15",
  "4.1" -> "2.13.15"
)

val sparkVersion =
  sys.props.get("spark.version").getOrElse("3.3")

val sparkSqlVersion =
  sparkSqlVersions.getOrElse(
    sparkVersion,
    sys.error(
      s"Unsupported spark.version: $sparkVersion. " +
        s"Supported versions are: ${sparkSqlVersions.keys.mkString(", ")}"
    )
  )

val benchmarkScalaVersion =
  sparkScalaVersions.getOrElse(
    sparkVersion,
    sys.error(
      s"Unsupported spark.version: $sparkVersion. " +
        s"Supported versions are: ${sparkScalaVersions.keys.mkString(", ")}"
    )
  )

ThisBuild / scalaVersion := benchmarkScalaVersion

lazy val root = (project in file("."))
  .settings(
    name := "spanner-spark-benchmark",

    resolvers += Resolver.mavenLocal,

    dependencyOverrides ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.16",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.2"
    ),

    libraryDependencies ++= Seq(
      "com.google.cloud.spark.spanner" %
        s"spark-$sparkVersion-spanner" %
        "0.0.1-SNAPSHOT",

      "org.apache.spark" %%
        "spark-sql" %
        sparkSqlVersion %
        "provided",

      "com.typesafe.play" %%
        "play-json" %
        "2.9.2"
    ),

    Test / parallelExecution := false,

    // sbt-assembly settings
    assembly / assemblyShadeRules := Seq(
      ShadeRule
        .rename(
          "com.google.common.**" ->
            "com.google.cloud.spark.spanner.shaded.com.google.common.@1"
        )
        .inAll,

      ShadeRule
        .rename(
          "com.google.protobuf.**" ->
            "com.google.cloud.spark.spanner.shaded.com.google.protobuf.@1"
        )
        .inAll
    ),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*)
        if xs.exists(_.endsWith(".SF")) ||
          xs.exists(_.endsWith(".DSA")) ||
          xs.exists(_.endsWith(".RSA")) =>
        MergeStrategy.discard

      case PathList(
        "META-INF",
        "services",
        "org.apache.spark.sql.sources.DataSourceRegister"
      ) =>
        MergeStrategy.concat

      case "reference.conf" =>
        MergeStrategy.concat

      case _ =>
        MergeStrategy.first
    },

    Compile / mainClass :=
      Some("com.google.cloud.spark.spanner.SparkSpannerWriteBenchmark"),

    spannerUp :=
      (BenchmarkingTasks.spannerUp.evaluated),

    spannerDown :=
      (BenchmarkingTasks.spannerDown.evaluated)
  )
  .settings(BenchmarkingTasks.customTaskSettings)