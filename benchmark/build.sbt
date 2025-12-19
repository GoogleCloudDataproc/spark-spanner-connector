import scala.sys.process._
//
// spanner-spark-tests
//
ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "spanner-spark-benchmark",
    resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
    libraryDependencies ++= Seq(
      "com.google.cloud.spark.spanner" % "spark-3.3-spanner" % "0.0.1-SNAPSHOT",
      "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided"
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

lazy val createSpannerInstance = inputKey[Unit]("Creates a spanner instance")

createSpannerInstance := {
  import scala.util.Try

  val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
  
  var instanceName: Option[String] = None
  var region: Option[String] = None
  var processingUnits: Int = 1000 // Default value

  val argsIterator = args.iterator
  while (argsIterator.hasNext) {
    val arg = argsIterator.next()
    arg match {
      case "--instanceName" if argsIterator.hasNext => instanceName = Some(argsIterator.next())
      case "--region" if argsIterator.hasNext => region = Some(argsIterator.next())
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

  (instanceName, region) match {
    case (Some(name), Some(r)) =>
      val projectId = "gcloud config get-value project".!!.trim
      // Note: As of now, gcloud spanner instances create does not support --region.
      // The region is determined by the config.
      // We will use the config for the region.
      val command = Seq(
        "gcloud", "spanner", "instances", "create", name,
        s"--project=$projectId",
        s"--config=regional-$r",
        s"--description=$name",
        s"--processing-units=$processingUnits"
      )

      println(s"Running command: ${command.mkString(" ")}")
      val exitCode = command.!
      if (exitCode != 0) {
        sys.error(s"Failed to create Spanner instance '$name'.")
      } else {
        println(s"Successfully initiated creation of Spanner instance '$name'.")
      }

    case (None, _) =>
      sys.error("Error: --instanceName is required.")
    case (_, None) =>
      sys.error("Error: --region is required.")
  }
}



// Define a new task to build the databricks test JAR
lazy val buildBenchmarkJar = taskKey[File]("Builds the spanner test suite JAR.")
buildBenchmarkJar := (assembly).value

// Define a new input task to run the job on Dataproc
lazy val runDataproc = inputKey[Unit]("Runs the spark job on Google Cloud Dataproc")

runDataproc := {
  // 1. Build the fat JAR
  val appJar = (assembly).value

  val mc = (Compile / mainClass).value.getOrElse(throw new RuntimeException("mainClass not found"))
  val mainClassArgs = Def.spaceDelimited("<arg>").parsed

  // 2. Get Dataproc configuration from environment variables
  val cluster = sys.env.getOrElse("SPANNER_DATAPROC_CLUSTER", throw new RuntimeException("SPANNER_DATAPROC_CLUSTER environment variable not set."))
  val region = sys.env.getOrElse("SPANNER_DATAPROC_REGION", throw new RuntimeException("SPANNER_DATAPROC_REGION environment variable not set."))
  val bucketName = sys.env.getOrElse("SPANNER_DATAPROC_BUCKET", throw new RuntimeException("SPANNER_DATAPROC_BUCKET environment variable not set."))
  val projectId = sys.env.getOrElse("SPANNER_PROJECT_ID", throw new RuntimeException("SPANNER_PROJECT_ID environment variable not set."))
  val bucketUri = s"gs://$bucketName"

  // 3. Create a unique upload directory for this run
  val runId = java.util.UUID.randomUUID().toString.take(8)
  val gcsPath = s"$bucketUri/connector-test-$runId"
  
  // 4. Upload the fat JAR to GCS
  val dest = s"$gcsPath/${appJar.getName}"
  println(s"Uploading ${appJar.getAbsolutePath} to $dest")
  s"gcloud storage cp ${appJar.getAbsolutePath} $dest".!

  // 5. Construct the benchmark arguments
  val benchmarkArgs = mainClassArgs ++ Seq(projectId)

  // 6. Construct the gcloud dataproc command
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
}

// Define a new input task to create a Dataproc cluster
lazy val createDataprocCluster = inputKey[Unit]("Creates a Google Cloud Dataproc cluster.")

createDataprocCluster := {
  import scala.util.Try

  val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
  
  var clusterName: Option[String] = None
  var region: Option[String] = None
  var numWorkers: Int = 2 // Default value
  var masterMachineType: String = "n2-standard-4" // Default value
  var workerMachineType: String = "n2-standard-4" // Default value
  var imageVersion: String = "2.1-debian11" // Default value
  
  val argsIterator = args.iterator
  while (argsIterator.hasNext) {
    val arg = argsIterator.next()
    arg match {
      case "--clusterName" if argsIterator.hasNext => clusterName = Some(argsIterator.next())
      case "--region" if argsIterator.hasNext => region = Some(argsIterator.next())
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

  val regionToUse = region.getOrElse(sys.env.getOrElse("SPANNER_DATAPROC_REGION", "us-central1"))
  val bucketName = sys.env.getOrElse("SPANNER_DATAPROC_BUCKET", throw new RuntimeException("SPANNER_DATAPROC_BUCKET environment variable not set."))
  val projectId = sys.env.getOrElse("SPANNER_PROJECT_ID", "gcloud config get-value project".!!.trim)

  clusterName match {
    case Some(name) =>
      println(s"Attempting to create Dataproc cluster '$name' in project '$projectId' region '$regionToUse'...")
      // TODO be able to create the cluster with sdd
      val command = Seq(
        "gcloud", "dataproc", "clusters", "create", name,
        s"--project=$projectId",
        s"--region=$regionToUse",
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
        sys.error(s"Failed to create Dataproc cluster '$name'. It may already exist or you lack permissions.")
      } else {
        println(s"Successfully initiated creation of Dataproc cluster '$name'.")
      }

    case None =>
      sys.error("Error: --clusterName is required.")
  }
}


// Define a new input task to run the job on Databricks
lazy val runDatabricks = inputKey[Unit]("Runs the spark job on Databricks")

runDatabricks := {
  // 1. Build the fat JAR
  val appJar = (assembly).value

  val mc = (Compile / mainClass).value.getOrElse(throw new RuntimeException("mainClass not found"))
  val clusterId = sys.env.getOrElse("SPANNER_DATABRICKS_CLUSTER_ID", throw new RuntimeException("SPANNER_DATABRICKS_CLUSTER_ID environment variable not set."))
  val mainClassArgs = Def.spaceDelimited("<arg>").parsed

  // 2. Create a unique upload directory for this run
  val runId = java.util.UUID.randomUUID().toString.take(8)
  val dbfsPath = s"dbfs:/FileStore/sbt-runs/$runId"
  s"databricks fs mkdirs $dbfsPath".!

  // 3. Upload the fat JAR
  val dest = s"$dbfsPath/${appJar.getName}"
  println(s"Uploading ${appJar.getAbsolutePath} to $dest")
  s"databricks fs cp ${appJar.getAbsolutePath} $dest".!
  val dbfsJarPath = s"""{"jar": "$dest"}"""

  // 4. Construct the job submission JSON manually
  val parametersJson = mainClassArgs.map(arg => s""""$arg"""").mkString(",")
  val jobJson = s"""
  {
    "run_name": "sbt-run-$runId",
    "existing_cluster_id": "$clusterId",
    "spark_jar_task": {
      "main_class_name": "$mc",
      "parameters": [$parametersJson]
    },
    "libraries": [$dbfsJarPath]
  }
  """

  println("Submitting Databricks job with JSON:")
  println(jobJson)

  // 5. Submit the job
  val tempFile = java.io.File.createTempFile("databricks-job", ".json")
  IO.write(tempFile, jobJson)
  s"databricks jobs submit --json-file ${tempFile.getAbsolutePath}".!
  tempFile.delete()
}

// Define a new task to create a Databricks instance pool
lazy val createDatabricksPool = taskKey[Unit]("Creates a Databricks instance pool for testing.")

createDatabricksPool := {
  // --- Configuration for the Instance Pool ---
  val poolName = "spanner-spark-test-pool"
  val nodeTypeId = "i3.xlarge" // Example node type, change as needed
  val minIdleInstances = 0
  val maxCapacity = 10
  val idleTimeoutMinutes = 15
  val sparkVersion = "13.3.x-scala2.12" // Updated to a modern, compliant runtime

  println(s"Attempting to create instance pool '$poolName'...")

  val poolJson = s"""
  {
    "instance_pool_name": "$poolName",
    "node_type_id": "$nodeTypeId",
    "min_idle_instances": $minIdleInstances,
    "max_capacity": $maxCapacity,
    "idle_instance_autotermination_minutes": $idleTimeoutMinutes,
    "enable_elastic_disk": true,
    "preloaded_spark_versions": ["$sparkVersion"]
  }
  """

  println("Using JSON configuration:")
  println(poolJson)
  
  val tempFile = java.io.File.createTempFile("databricks-pool", ".json")
  IO.write(tempFile, poolJson)
  val exitCode = s"databricks instance-pools create --json-file ${tempFile.getAbsolutePath}".!
  tempFile.delete()

  if (exitCode != 0) {
    sys.error(s"Failed to create Databricks instance pool '$poolName'. It may already exist.")
  } else {
    println(s"Successfully initiated creation of instance pool '$poolName'.")
  }
}

// Define a new task to create a Databricks cluster based on an instance pool
lazy val createDatabricksCluster = taskKey[Unit]("Creates a Databricks cluster using an instance pool for testing.")

createDatabricksCluster := {
  // --- Configuration for the Cluster ---
  val clusterName = "spanner-spark-test-cluster"
  val instancePoolId = "1204-052313-punch3-pool-fkarvzf6" // Must match the pool created by createDatabricksPool
  val sparkVersion = "13.3.x-scala2.12" // Updated to a modern, compliant runtime
  val numWorkers = 2 // Number of workers for the cluster
  val autoterminationMinutes = 30 // Auto-terminate after 30 minutes of inactivity

  println(s"Attempting to create cluster '$clusterName' using instance pool '$instancePoolId'...")

  val clusterJson = s"""
  {
    "cluster_name": "$clusterName",
    "spark_version": "$sparkVersion",
    "instance_pool_id": "$instancePoolId",
    "num_workers": $numWorkers,
    "autotermination_minutes": $autoterminationMinutes
  }
  """

  println("Using JSON configuration:")
  println(clusterJson)

  val tempFile = java.io.File.createTempFile("databricks-cluster", ".json")
  IO.write(tempFile, clusterJson)
  val exitCode = s"databricks clusters create --json @${tempFile.getAbsolutePath}".!
  tempFile.delete()

  if (exitCode != 0) {
    sys.error(s"Failed to create Databricks cluster '$clusterName'. It may already exist or pool '$instancePoolId' does not exist.")
  } else {
    println(s"Successfully initiated creation of cluster '$clusterName'.")
    println(s"You can now set DATABRICKS_CLUSTER_ID to the ID of this new cluster (find it in the Databricks UI) and run 'sbt runDatabricks'.")
  }
}

// Define a new task to create a Databricks cluster WITHOUT an instance pool
lazy val createClusterNoPool = taskKey[Unit]("Creates a Databricks cluster without an instance pool for testing.")

createClusterNoPool := {
  // --- Configuration for the Cluster ---
  val clusterName = "spanner-spark-test-cluster-no-pool"
  val sparkVersion = "13.3.x-scala2.12"
  val nodeTypeId = "i3.xlarge"
  val numWorkers = 2
  val autoterminationMinutes = 30

  // Get the required IAM Role ARN from an environment variable
  val iamRoleArn = sys.env.getOrElse("DATABRICKS_IAM_ROLE_ARN", throw new RuntimeException("DATABRICKS_IAM_ROLE_ARN environment variable not set."))

  println(s"Attempting to create cluster '$clusterName' without an instance pool...")

  val clusterJson = s"""
  {
    "cluster_name": "$clusterName",
    "spark_version": "$sparkVersion",
    "node_type_id": "$nodeTypeId",
    "num_workers": $numWorkers,
    "autotermination_minutes": $autoterminationMinutes,
    "aws_attributes": {
      "instance_profile_arn": "$iamRoleArn"
    }
  }
  """

  println("Using JSON configuration:")
  println(clusterJson)

  val tempFile = java.io.File.createTempFile("databricks-cluster-no-pool", ".json")
  IO.write(tempFile, clusterJson)
  val exitCode = s"databricks clusters create --json-file ${tempFile.getAbsolutePath}".!
  tempFile.delete()

  if (exitCode != 0) {
    sys.error(s"Failed to create Databricks cluster '$clusterName'. It may already exist.")
  } else {
    println(s"Successfully initiated creation of cluster '$clusterName'.")
  }
}

