import sbt._
import Keys._
import scala.sys.process._
import complete.DefaultParsers._
import sbtassembly.AssemblyKeys.assembly
import play.api.libs.json._

object CustomTasks {

  lazy val createSpannerInstance = inputKey[Unit]("Creates a spanner instance")
  lazy val createSpannerDatabase = inputKey[Unit]("Creates a spanner database")
  lazy val buildBenchmarkJar = taskKey[File]("Builds the spanner test suite JAR.")
  lazy val runDataproc = inputKey[Unit]("Runs the spark job on Google Cloud Dataproc")
  lazy val createDataprocCluster = inputKey[Unit]("Creates a Google Cloud Dataproc cluster.")
  lazy val createSpannerTable = inputKey[Unit]("Creates a Spanner table.")

  private def loadBenchmarkConfig(file: File): JsValue = {
    Json.parse(IO.read(file))
  }

  lazy val customTaskSettings: Seq[Setting[_]] = Seq(
    createSpannerInstance := {
      import scala.util.Try

      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)
      
      val instanceName = (config \ "instanceId").as[String]
      val projectId = (config \ "projectId").as[String]
      var spannerRegionFromConfig: Option[String] = (config \ "spannerRegion").asOpt[String]
      var processingUnits: Int = 1000 // Default value

      val argsIterator = args.drop(1).iterator // Drop config file path
      while (argsIterator.hasNext) {
        val arg = argsIterator.next()
        arg match {
          case "--region" if argsIterator.hasNext => spannerRegionFromConfig = Some(argsIterator.next())
          case "--processingUnits" if argsIterator.hasNext =>
            val next = argsIterator.next()
            Try(next.toInt).toOption match {
              case Some(value) => processingUnits = value
              case None => sys.error(s"Invalid value for --processingUnits: '$next'. Must be an integer.")
            }
          case other if other.startsWith("--") => sys.error(s"Unknown option: $other")
          case _ => // Ignore non-option arguments
        }
      }

      spannerRegionFromConfig match {
        case Some(r) =>
          val command = Seq(
            "gcloud", "spanner", "instances", "create", instanceName,
            s"--project=$projectId",
            s"--config=regional-$r",
            s"--description=$instanceName",
            s"--processing-units=$processingUnits"
          )

          println(s"Running command: ${command.mkString(" ")}")
          val exitCode = command.!
          if (exitCode != 0) {
            sys.error(s"Failed to create Spanner instance '$instanceName'.")
          } else {
            println(s"Successfully initiated creation of Spanner instance '$instanceName'.")
          }

        case None =>
          sys.error("Error: --region or 'spannerRegion' in config is required.")
      }
    },

    createSpannerDatabase := {
      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)

      val instanceId = (config \ "instanceId").as[String]
      val databaseId = (config \ "databaseId").as[String]
      val projectId = (config \ "projectId").as[String]

      val command = Seq(
        "gcloud", "spanner", "databases", "create", databaseId,
        s"--instance=$instanceId",
        s"--project=$projectId"
      )

      println(s"Running command: ${command.mkString(" ")}")
      val exitCode = command.!
      if (exitCode != 0) {
        sys.error(s"Failed to create Spanner database '$databaseId' in instance '$instanceId'.")
      } else {
        println(s"Successfully initiated creation of Spanner database '$databaseId' in instance '$instanceId'.")
      }
    },

    buildBenchmarkJar := (assembly in ThisProject).value,

    runDataproc := {
      val appJar = (assembly in ThisProject).value
      val mc = (Compile / mainClass).value.getOrElse(throw new RuntimeException("mainClass not found"))
      
      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)

      val mainClassArgs = args.drop(1) // Drop config file path

      val cluster = (config \ "dataprocCluster").asOpt[String].getOrElse(sys.error("dataprocCluster not found in config"))
      val region = (config \ "dataprocRegion").asOpt[String].getOrElse(sys.error("dataprocRegion not found in config"))
      val bucketName = (config \ "dataprocBucket").as[String]
      val projectId = (config \ "projectId").as[String]
      val bucketUri = s"gs://$bucketName"

      val runId = java.util.UUID.randomUUID().toString.take(8)
      val gcsPath = s"$bucketUri/connector-test-$runId"
      
      val dest = s"$gcsPath/${appJar.getName}"
      println(s"Uploading ${appJar.getAbsolutePath} to $dest")
      s"gcloud storage cp ${appJar.getAbsolutePath} $dest".!

      val benchmarkArgs = mainClassArgs ++ Seq(projectId)

      val command = Seq(
        "gcloud", "dataproc", "jobs", "submit", "spark",
        s"--cluster=$cluster",
        s"--region=$region",
        s"--project=$projectId",
        s"--class=$mc",
        s"--jars=$dest",
        "--"
      ) ++ benchmarkArgs

      println(s"Submitting Dataproc job: ${command.mkString(" ")}")
      command.!
    },

    createDataprocCluster := {
      import scala.util.Try
      
      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)
      
      val clusterName = (config \ "dataprocCluster").asOpt[String].getOrElse(sys.error("dataprocCluster not found in config"))
      val region = (config \ "dataprocRegion").asOpt[String].getOrElse("us-central1")
      var numWorkers: Int = 2 // Default value
      var masterMachineType: String = "n2-standard-4" // Default value
      var workerMachineType: String = "n2-standard-4" // Default value
      var imageVersion: String = "2.1-debian11" // Default value
      
      val argsIterator = args.drop(1).iterator // Drop config file path
      while (argsIterator.hasNext) {
        val arg = argsIterator.next()
        arg match {
          case "--numWorkers" if argsIterator.hasNext =>
            val next = argsIterator.next()
            Try(next.toInt).toOption match {
              case Some(value) => numWorkers = value
              case None => sys.error(s"Invalid value for --numWorkers: '$next'. Must be an integer.")
            }
          case "--masterMachineType" if argsIterator.hasNext => masterMachineType = argsIterator.next()
          case "--workerMachineType" if argsIterator.hasNext => workerMachineType = argsIterator.next()
          case "--imageVersion" if argsIterator.hasNext => imageVersion = argsIterator.next()
          case other if other.startsWith("--") => sys.error(s"Unknown option: $other")
          case _ => // Ignore non-option arguments
        }
      }

      val bucketName = (config \ "dataprocBucket").as[String]
      val projectId = (config \ "projectId").as[String]

      println(s"Attempting to create Dataproc cluster '$clusterName' in project '$projectId' region '$region'...")
      val command = Seq(
        "gcloud", "dataproc", "clusters", "create", clusterName,
        s"--project=$projectId",
        s"--region=$region",
        s"--bucket=$bucketName",
        s"--no-address",
        s"--num-workers=$numWorkers",
        s"--image-version=$imageVersion",
        "--enable-component-gateway",
        s"--master-machine-type=$masterMachineType",
        "--master-boot-disk-type=hyperdisk-balanced",
        "--master-boot-disk-size=100",
        s"--worker-machine-type=$workerMachineType",
        "--worker-boot-disk-type=hyperdisk-balanced",
        "--worker-boot-disk-size=200",
        "--scopes=https://www.googleapis.com/auth/cloud-platform"
      )

      println(s"Running command: ${command.mkString(" ")}")
      val exitCode = command.!(ProcessLogger(line => println(line)))

      if (exitCode != 0) {
        sys.error(s"Failed to create Dataproc cluster '$clusterName'. It may already exist or you lack permissions.")
      } else {
        println(s"Successfully initiated creation of Dataproc cluster '$clusterName'.")
      }
    },
    
    createSpannerTable := {
      val args = Def.spaceDelimited("<arg>").parsed
      val configFile = args.headOption.map(file).getOrElse(baseDirectory.value / "benchmark.json")
      val config = loadBenchmarkConfig(configFile)

      val tableName = (config \ "writeTable").as[String]
      val ddlFile = (baseDirectory.value / "ddl" / "create_test_table.sql")
      val ddlContent = IO.read(ddlFile).replace("TransferTest", tableName)
      
      val instanceId = (config \ "instanceId").as[String]
      val databaseId = (config \ "databaseId").as[String]
      val projectId = (config \ "projectId").as[String]
      
      val command = Seq(
        "gcloud", "spanner", "databases", "ddl", "update", databaseId,
        s"--instance=$instanceId",
        s"--project=$projectId",
        s"--ddl=$ddlContent"
      )
      
      println(s"Executing DDL to create table '$tableName' in database '$databaseId':")
      println(ddlContent)
      
      val exitCode = command.!
      if (exitCode != 0) {
        sys.error(s"Failed to create table '$tableName'.")
      } else {
        println(s"Successfully created table '$tableName'.")
      }
    }
  )
}
