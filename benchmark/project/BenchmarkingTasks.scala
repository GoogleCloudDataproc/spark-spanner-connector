import sbt._
import Keys._
import scala.sys.process._
import sbtassembly.AssemblyKeys.assembly
import play.api.libs.json._

object BenchmarkingTasks {

  lazy val buildBenchmarkJar = taskKey[File]("Builds the spanner test suite JAR.")
  lazy val runDatabricksNotebook = inputKey[Unit]("Runs a notebook on Databricks.")
  lazy val createResultsBucket = inputKey[Unit]("Creates a GCS bucket for benchmark results.")
  lazy val refreshDatabricksToken = inputKey[Unit]("Refreshes the Databricks token.")
  lazy val spannerUp = inputKey[Unit]("Ensures spanner instance, database and table exist for benchmark.")
  lazy val spannerDown = inputKey[Unit]("Removes the spanner instance, and its databases, referenced in the benchmark config.")
  lazy val createBenchmarkSpannerTable = inputKey[Unit]("Creates the Spanner table required for a specific benchmark scenario.")
  lazy val runBenchmark = inputKey[Unit]("Runs a specified benchmark scenario in a given environment.")
  lazy val setBenchmarkBaseline = inputKey[Unit]("Sets a specific benchmark run as the baseline.")
  lazy val compareBenchmarkResults = inputKey[Unit]("Compares a specific benchmark run against the baseline.")

  private def loadBenchmarkConfig(file: File): JsValue = {
    Json.parse(IO.read(file))
  }

  // Common helper to get benchmark and environment info
  private def getBenchmarkContext(benchmarkName: String, baseDir: File): (JsObject, JsObject, String) = {
    val envFile = baseDir / "environment.json"
    if (!envFile.exists()) sys.error(s"Environment file not found at ${envFile.getAbsolutePath}.")
    val environmentConfig = loadBenchmarkConfig(envFile)

    val defsFile = baseDir / "benchmark_definitions.json"
    if (!defsFile.exists()) sys.error(s"Benchmark definitions file not found at ${defsFile.getAbsolutePath}.")
    val benchmarkDefs = (Json.parse(IO.read(defsFile)) \ "benchmarks").as[JsArray]

    val benchmarkDef = benchmarkDefs.value.find(b => (b \ "name").as[String] == benchmarkName).getOrElse {
      sys.error(s"Benchmark definition for '$benchmarkName' not found in benchmark_definitions.json.")
    }.as[JsObject]

    val environmentType = (benchmarkDef \ "environment").asOpt[String].getOrElse {
      sys.error(s"Benchmark '$benchmarkName' does not have an 'environment' field in its definition.")
    }

    val specificEnvConfig = (environmentConfig \ environmentType).asOpt[JsObject].getOrElse {
      sys.error(s"Configuration for environment '$environmentType' not found in environment.json.")
    }

    (benchmarkDef, specificEnvConfig, environmentType)
  }

  // Helper to derive the write table name
  private def deriveWriteTableName(benchmarkName: String): String = {
    val sanitizedName = benchmarkName.replaceAll("[^a-zA-Z0-9_]", "_")
    s"benchmark_${sanitizedName}_dest"
  }


  private def runDatabricksNotebookHelper(config: JsValue, baseDirectory: File): Unit = {
    val databricksHost = (config \ "databricksHost").as[String]
    val databricksToken = (config \ "databricksToken").as[String]
    val clusterId = (config \ "clusterId").as[String]
    val baseDatabricksWorkspacePath = (config \ "notebookPath").asOpt[String].getOrElse("/Shared") // Base path in Databricks Workspace
    val localNotebookFilePath = (config \ "localNotebookFilePath").as[String] // Path to the local notebook file
    val notebookBasename = new java.io.File(localNotebookFilePath).getName
    val notebookPath = s"${baseDatabricksWorkspacePath.stripSuffix("/")}/$notebookBasename"

    // 1. Import notebook
    val language = localNotebookFilePath.substring(localNotebookFilePath.lastIndexOf('.') + 1).toUpperCase match {
      case "PY" => "PYTHON"
      case "SQL" => "SQL"
      case "R" => "R"
      case "SCALA" => "SCALA"
      case other => sys.error(s"Unsupported notebook language extension: .$other")
    }

    println(s"Importing notebook $localNotebookFilePath to $notebookPath on Databricks...")
    val importCommand = Seq(
      "databricks", "workspace", "import", notebookPath,
      "--file", (baseDirectory / localNotebookFilePath).toString,
      "--language", language,
      "--format", "SOURCE",
      "--overwrite"
    )
    println(s"Executing command: ${importCommand.mkString(" ")}")
    val importExitCode = Process(importCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!
    if (importExitCode != 0) {
      sys.error(s"Failed to import notebook to Databricks.")
    }
    println("Notebook imported successfully.")

    // 2. Prepare parameters
    val databricksKeys = Set("databricksHost", "databricksToken", "clusterId", "notebookPath", "localNotebookFilePath", "localPrepareNotebookPath", "ucVolumePath")
    val allParams = config.as[JsObject].value.filterKeys(k => !databricksKeys.contains(k))
    val baseParameters = JsObject(
      allParams.map { case (key, value) =>
        key -> (value match {
          case s: JsString => s
          case other => Json.toJson(other.toString)
        })
      }.toSeq
    )

    // 3. Run notebook
    val jobJson = Json.obj(
      "run_name" -> "Spark Spanner Benchmark",
      "tasks" -> Json.arr(
        Json.obj(
          "task_key" -> "benchmark_task",
          "notebook_task" -> Json.obj(
            "notebook_path" -> notebookPath,
            "source" -> "WORKSPACE",
            "base_parameters" -> baseParameters
          ),
          "existing_cluster_id" -> clusterId
        )
      )
    )
    val jobJsonString = Json.stringify(jobJson)

      println(s"Submitting job for notebook $notebookPath on cluster $clusterId...")
      val runCommand = Seq(
        "databricks", "jobs", "submit", "--json", jobJsonString
      )
      println(s"Executing command: databricks jobs submit --json '...' and capturing output.")
      
      // Capture the output of the command
      val output = Process(runCommand, None, "DATABRICKS_HOST" -> databricksHost, "DATABRICKS_TOKEN" -> databricksToken).!!.trim
      
      // Parse the JSON output
      val resultJson = Json.parse(output)
      val runId = (resultJson \ "run_id").as[Long]
      val runPageUrl = (resultJson \ "run_page_url").as[String]
      
      println(s"Job submitted successfully. Run ID: $runId")
      println(s"Monitor progress at: $runPageUrl")
    }
  
    lazy val customTaskSettings: Seq[Setting[_]] = Seq(
      runBenchmark := {
        import scala.util.Try

        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.isEmpty) {
          sys.error("Usage: sbt \"runBenchmark <benchmarkName>\"")
        }
        val benchmarkName = args(0)
        val baseDir = baseDirectory.value
        val (benchmarkDef, specificEnvConfig, environmentType) = getBenchmarkContext(benchmarkName, baseDir)

        // Load data sources to resolve source table
        val dataSourcesFile = baseDir / "data_sources.json"
        if (!dataSourcesFile.exists()) {
          sys.error(s"Data sources file not found at ${dataSourcesFile.getAbsolutePath}.")
        }
        val dataSources = (Json.parse(IO.read(dataSourcesFile)) \ "dataSources").as[JsArray]

        val logicalDataSourceName = (benchmarkDef \ "dataSource").asOpt[String]
        var resolvedSourceTable: Option[String] = None

        logicalDataSourceName.foreach {
          dsName =>
          val dataSourceMappings = (specificEnvConfig \ "dataSourceMappings").asOpt[JsObject].getOrElse(Json.obj())
          resolvedSourceTable = (dataSourceMappings \ dsName).asOpt[String]
          if (resolvedSourceTable.isEmpty) {
              sys.error(s"Physical table mapping for logical data source '$dsName' not found in environment.json for environment '$environmentType'.")
          }
        }

        // --- Generate writeTableName ---
        val physicalWriteTableName = deriveWriteTableName(benchmarkName)


        // Merge configurations
        var tempConfig = benchmarkDef.deepMerge(specificEnvConfig)
        tempConfig = tempConfig - "writeTableName" + ("writeTable" -> Json.toJson(physicalWriteTableName))
        resolvedSourceTable.foreach(s => tempConfig = tempConfig + ("sourceTable" -> Json.toJson(s)))
        tempConfig = tempConfig + ("buildSparkVersion" -> Json.toJson(sys.props.get("spark.version").getOrElse("3.3")))
        
        val finalMergedConfig = tempConfig
        val configJsonString = Json.stringify(finalMergedConfig - "databricksToken") // Do not log Databricks token
        println(s"Running benchmark with merged configuration: ${configJsonString}")

        // Execute the Spark job
        environmentType match {
          case "dataproc" =>
            val appJar = (assembly in ThisProject).value
            val mc = (Compile / mainClass).value.getOrElse(throw new RuntimeException("mainClass not found"))

            val cluster = (finalMergedConfig \ "dataprocCluster").as[String]
            val region = (finalMergedConfig \ "dataprocRegion").as[String]
            val bucketName = (finalMergedConfig \ "dataprocBucket").as[String]
            val projectId = (finalMergedConfig \ "projectId").as[String]

            val runId = java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now()) + "_" + java.util.UUID.randomUUID().toString.take(8)
            val gcsPath = s"gs://$bucketName/connector-test-$runId"
            
            val dest = s"$gcsPath/${appJar.getName}"
            println(s"Uploading ${appJar.getAbsolutePath} to $dest")
            s"gcloud storage cp ${appJar.getAbsolutePath} $dest".!

            val command = Seq(
              "gcloud", "dataproc", "jobs", "submit", "spark",
              s"--cluster=$cluster",
              s"--region=$region",
              s"--project=$projectId",
              s"--class=$mc",
              s"--jars=$dest",
              "--"
            ) ++ Seq(configJsonString)

            println(s"Submitting Dataproc job: ${command.mkString(" ")}")
            
            val jobOutput = new StringBuilder
            val errorOutput = new StringBuilder
            val exitCode = command.!(ProcessLogger(
              line => {
                println(line)
                jobOutput.append(line).append("\n")
              },
              line => {
                System.err.println(line)
                errorOutput.append(line).append("\n")
              }
            ))

            if (exitCode != 0) {
              sys.error(s"Dataproc job submission failed with exit code $exitCode.\nStderr:\n${errorOutput.toString}")
            }

            val resultPathPattern = """Writing results to (gs://[^\s]+)""" .r
            resultPathPattern.findFirstMatchIn(jobOutput.toString).map(_.group(1)) match {
              case Some(path) =>
                println("\n" + ("-" * 50))
                println("Benchmark Run Complete")
                println(s"Result file created at: $path")
                println(("-" * 50) + "\n")
              case None =>
                println("\nWarning: Could not automatically find the results file path in the Dataproc job output.")
            }

          case "databricks" =>
            val notebookPath = (benchmarkDef \ "localNotebookPath").as[String]
            val configWithLocalPath = finalMergedConfig + ("localNotebookFilePath" -> Json.toJson(notebookPath))
            runDatabricksNotebookHelper(configWithLocalPath, baseDir)

          case _ =>
            sys.error(s"Unsupported environment type: '$environmentType'.")
        }
      },
      
      setBenchmarkBaseline := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.length < 2) {
          sys.error("Usage: sbt \"setBenchmarkBaseline <benchmarkName> <gcsPath>\"")
        }
        val benchmarkName = args.head
        val sourceGcsPath = args(1)
        val baseDir = baseDirectory.value
        
        val (_, specificEnvConfig, _) = getBenchmarkContext(benchmarkName, baseDir)
        val resultsBucket = (specificEnvConfig \ "resultsBucket").as[String]
        
        val baselineGcsPath = s"gs://${resultsBucket}/SparkSpannerWriteBenchmark/${benchmarkName}-baseline.json"
        
        println(s"--- Setting Baseline for ${benchmarkName} ---")
        println(s"Copying ${sourceGcsPath} to ${baselineGcsPath}")
        
        val command = Seq("gsutil", "cp", sourceGcsPath, baselineGcsPath)
        if (command.! != 0) {
          sys.error("Failed to set baseline.")
        }
        println("Baseline set successfully.")
      },

      compareBenchmarkResults := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.length < 2) {
          sys.error("Usage: sbt \"compare <benchmarkName> <gcsPath>\"")
        }
        val benchmarkName = args.head
        val currentGcsPath = args(1)
        val baseDir = baseDirectory.value

        val (_, specificEnvConfig, _) = getBenchmarkContext(benchmarkName, baseDir)
        val resultsBucket = (specificEnvConfig \ "resultsBucket").as[String]
        
        val baselineGcsPath = s"gs://${resultsBucket}/SparkSpannerWriteBenchmark/${benchmarkName}-baseline.json"
        
        val tempDir = IO.createTemporaryDirectory
        
        println(s"--- Comparing ${benchmarkName} (run ${currentGcsPath}) against baseline ---")
        println(s"Downloading files to temporary directory: ${tempDir.getAbsolutePath}")
        
        val baselineFile = tempDir / "baseline.json"
        val currentFile = tempDir / "current.json"

        val gsutilCpBaseline = Seq("gsutil", "cp", baselineGcsPath, baselineFile.getAbsolutePath)
        val gsutilCpCurrent = Seq("gsutil", "cp", currentGcsPath, currentFile.getAbsolutePath)
        
        if (gsutilCpBaseline.! != 0) {
          IO.delete(tempDir)
          sys.error(s"Failed to download baseline file from GCS: ${baselineGcsPath}")
        }
        if (gsutilCpCurrent.! != 0) {
          IO.delete(tempDir)
          sys.error(s"Failed to download current result file from GCS: ${currentGcsPath}")
        }
        
        println("--- Generating Comparison Report ---")

        val baselineJson = Json.parse(IO.read(baselineFile))
        val currentJson = Json.parse(IO.read(currentFile))

        val baselineMetrics = (baselineJson \ "performanceMetrics").as[JsObject]
        val currentMetrics = (currentJson \ "performanceMetrics").as[JsObject]

        case class Metric(name: String, key: String, higherIsBetter: Boolean)
        val metricsToCompare = Seq(
          Metric("Duration (s)", "durationSeconds", higherIsBetter = false),
          Metric("Throughput (MB/s)", "throughputMbPerSec", higherIsBetter = true),
          Metric("Records Written", "recordCount", higherIsBetter = true)
        )

        def formatChange(baseline: Double, current: Double, higherIsBetter: Boolean): String = {
          if (baseline == 0) "N/A"
          else {
            val change = ((current - baseline) / baseline) * 100
            val emoji = if (change == 0) {
              "➡️"
            } else {
              val isImprovement = (change > 0 && higherIsBetter) || (change < 0 && !higherIsBetter)
              if (isImprovement) "📈" else "📉"
            }
            f"$change%+.2f%% $emoji"
          }
        }

        println("\n## 🚀 Spark Spanner Connector Benchmark Report\n")
        println("| Metric              | Baseline   | Current PR | Change      |")
        println("|---------------------|------------|------------|-------------|")

        metricsToCompare.foreach {
          metric =>
          val baselineValue = (baselineMetrics \ metric.key).asOpt[Double].getOrElse(0.0)
          val currentValue = (currentMetrics \ metric.key).asOpt[Double].getOrElse(0.0)
          val changeStr = formatChange(baselineValue, currentValue, metric.higherIsBetter)
          
          val formattedBaseline = f"$baselineValue%10.2f"
          val formattedCurrent = f"$currentValue%10.2f"
          
          println(f"| ${metric.name}%-19s | ${formattedBaseline} | ${formattedCurrent} | ${changeStr}%-11s |")
        }
        
        println("")
        
        IO.delete(tempDir)
      },

      createBenchmarkSpannerTable := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.isEmpty) {
          sys.error("Usage: sbt \"createBenchmarkSpannerTable <benchmarkName>\"")
        }
        val benchmarkName = args(0)
        val baseDir = baseDirectory.value

        val (benchmarkDef, specificEnvConfig, _) = getBenchmarkContext(benchmarkName, baseDir)
        
        val dataSourcesFile = baseDir / "data_sources.json"
        if (!dataSourcesFile.exists()) sys.error(s"Data sources file not found at ${dataSourcesFile.getAbsolutePath}.")
        val dataSources = (Json.parse(IO.read(dataSourcesFile)) \ "dataSources").as[JsArray]

        val projectId = (specificEnvConfig \ "projectId").as[String]
        val instanceId = (specificEnvConfig \ "instanceId").as[String]
        val databaseId = (specificEnvConfig \ "databaseId").as[String]
        val writeTableName = deriveWriteTableName(benchmarkName)

        val logicalDataSourceName = (benchmarkDef \ "dataSource").asOpt[String].getOrElse {
          sys.error(s"Benchmark '$benchmarkName' does not specify a 'dataSource' to infer DDL from.")
        }
        val dataSourceDef = dataSources.value.find(ds => (ds \ "name").as[String] == logicalDataSourceName).getOrElse {
          sys.error(s"Logical data source '$logicalDataSourceName' not found in data_sources.json.")
        }
        val ddlFile = (dataSourceDef \ "ddlFile").as[String]
        val ddlContent = IO.read(baseDir / ddlFile).replace("TransferTest", writeTableName)


        println(s"Checking for Spanner table '$writeTableName' in database '$databaseId'...")
        val checkTableCommand = Seq(
          "gcloud", "spanner", "databases", "ddl", "describe", databaseId,
          s"--instance=$instanceId",
          s"--project=$projectId"
        )
        val ddlOutput = checkTableCommand.!!
        if (ddlOutput.contains(s"CREATE TABLE $writeTableName")) {
          println(s"Table '$writeTableName' already exists in database '$databaseId'.")
        } else {
          println(s"Table '$writeTableName' not found, creating it...")
          val createTableCommand = Seq(
            "gcloud", "spanner", "databases", "ddl", "update", databaseId,
            s"--instance=$instanceId",
            s"--project=$projectId",
            s"--ddl=$ddlContent"
          )

          println(s"Executing DDL to create table '$writeTableName' in database '$databaseId':")
          println(ddlContent)

          if (createTableCommand.! != 0) {
            sys.error(s"Failed to create table '$writeTableName'.")
          } else {
            println(s"Successfully created table '$writeTableName'.")
          }
        }
      },

      spannerDown := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.isEmpty) {
          sys.error("Usage: sbt \"spannerDown <benchmarkName>\"")
        }
        val benchmarkName = args(0)
        val baseDir = baseDirectory.value
        val (_, specificEnvConfig, _) = getBenchmarkContext(benchmarkName, baseDir)

        val instanceId = (specificEnvConfig \ "instanceId").as[String]
        val projectId = (specificEnvConfig \ "projectId").as[String]

        println(s"Attempting to tear down Spanner instance '$instanceId' in project '$projectId'...")

        // 1. Check if instance exists
        val checkInstanceCommand = Seq("gcloud", "spanner", "instances", "describe", instanceId, s"--project=$projectId")
        if (checkInstanceCommand.! != 0) {
          println(s"Spanner instance '$instanceId' does not exist or you don't have permissions. Nothing to delete.")
        } else {
          // 2. List and delete databases within the instance
          println(s"Listing databases in instance '$instanceId'...")
          val listDatabasesCommand = Seq(
            "gcloud", "spanner", "databases", "list",
            s"--instance=$instanceId",
            s"--project=$projectId",
            "--format=json"
          )
          val databasesJson = Process(listDatabasesCommand).!!.trim
          val databases = Json.parse(databasesJson).as[JsArray]

          if (databases.value.isEmpty) {
            println(s"No databases found in instance '$instanceId'.")
          } else {
            databases.value.foreach { db =>
              val databaseId = (db \ "name").as[String].split("/").last
              println(s"Deleting database '$databaseId' from instance '$instanceId'...")
              val deleteDbCommand = Seq(
                "gcloud", "spanner", "databases", "delete", databaseId,
                s"--instance=$instanceId",
                s"--project=$projectId",
                "--quiet"
              )
              println(s"Running command: ${deleteDbCommand.mkString(" ")}")
              if (deleteDbCommand.! != 0) {
                println(s"Warning: Failed to delete database '$databaseId'. It might be already gone or permissions issue.")
              } else {
                println(s"Successfully initiated deletion of database '$databaseId'.")
              }
            }
          }

          // 3. Delete the Spanner instance
          println(s"Deleting Spanner instance '$instanceId'...")
          val deleteInstanceCommand = Seq(
            "gcloud", "spanner", "instances", "delete", instanceId,
            s"--project=$projectId",
            "--quiet"
          )
          println(s"Running command: ${deleteInstanceCommand.mkString(" ")}")
          if (deleteInstanceCommand.! != 0) {
            sys.error(s"Failed to delete Spanner instance '$instanceId'.")
          } else {
            println(s"Successfully initiated deletion of Spanner instance '$instanceId'.")
          }
        }
      },
      
      spannerUp := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.isEmpty) {
          sys.error("Usage: sbt \"spannerUp <benchmarkName>\"")
        }
        val benchmarkName = args.head
        val baseDir = baseDirectory.value
        val (benchmarkDef, specificEnvConfig, _) = getBenchmarkContext(benchmarkName, baseDir)

        val instanceId = (specificEnvConfig \ "instanceId").as[String]
        val projectId = (specificEnvConfig \ "projectId").as[String]
        val databaseId = (specificEnvConfig \ "databaseId").as[String]

        // 1. Ensure Spanner instance exists.
        println(s"Checking for Spanner instance '$instanceId'...")
        val checkInstanceCommand = Seq("gcloud", "spanner", "instances", "describe", instanceId, s"--project=$projectId")
        if (checkInstanceCommand.! != 0) {
          println(s"Spanner instance '$instanceId' not found, creating it...")
          val spannerRegion = (specificEnvConfig \ "spannerRegion").asOpt[String].getOrElse("us-central1")
          val createInstanceCommand = Seq(
            "gcloud", "spanner", "instances", "create", instanceId,
            s"--project=$projectId",
            s"--config=regional-$spannerRegion",
            s"--description=$instanceId",
            s"--autoscaling-min-processing-units=2000",
            s"--autoscaling-max-processing-units=20000",
            s"--autoscaling-high-priority-cpu-target=65",
            s"--autoscaling-storage-target=90",
            s"--edition=ENTERPRISE"
          )
          println(s"Running command: ${createInstanceCommand.mkString(" ")}")
          if (createInstanceCommand.! != 0) {
            sys.error(s"Failed to create Spanner instance '$instanceId'.")
          }
          println(s"Successfully initiated creation of Spanner instance '$instanceId'.")
        } else {
          println(s"Spanner instance '$instanceId' already exists.")
        }

        // 2. Ensure Spanner database exists.
        println(s"Checking for Spanner database '$databaseId' in instance '$instanceId'...")
        val checkDbCommand = Seq("gcloud", "spanner", "databases", "describe", databaseId, s"--instance=$instanceId", s"--project=$projectId")
        if (checkDbCommand.! != 0) {
          println(s"Spanner database '$databaseId' not found, creating it...")
          val createDbCommand = Seq(
            "gcloud", "spanner", "databases", "create", databaseId,
            s"--instance=$instanceId",
            s"--project=$projectId"
          )
          println(s"Running command: ${createDbCommand.mkString(" ")}")
          if (createDbCommand.! != 0) {
            sys.error(s"Failed to create Spanner database '$databaseId' in instance '$instanceId'.")
          }
          println(s"Successfully initiated creation of Spanner database '$databaseId'.")
        } else {
          println(s"Spanner database '$databaseId' already exists.")
        }

        // 3. Ensure Spanner table exists.
        val tableName = deriveWriteTableName(benchmarkName)
        println(s"Checking for Spanner table '$tableName' in database '$databaseId'...")
        val checkTableCommand = Seq(
          "gcloud", "spanner", "databases", "ddl", "describe", databaseId,
          s"--instance=$instanceId",
          s"--project=$projectId"
        )
        val ddlOutput = checkTableCommand.!!
        if (ddlOutput.contains(s"CREATE TABLE $tableName")) {
          println(s"Table '$tableName' already exists in database '$databaseId'.")
        } else {
          println(s"Table '$tableName' not found, creating it...")
          val dataSourcesFile = baseDir / "data_sources.json"
          if (!dataSourcesFile.exists()) sys.error(s"Data sources file not found at ${dataSourcesFile.getAbsolutePath}.")
          val dataSources = (Json.parse(IO.read(dataSourcesFile)) \ "dataSources").as[JsArray]

          val logicalDataSourceName = (benchmarkDef \ "dataSource").asOpt[String].getOrElse {
            sys.error(s"Benchmark '$benchmarkName' does not specify a 'dataSource' to infer DDL from.")
          }
          val dataSourceDef = dataSources.value.find(ds => (ds \ "name").as[String] == logicalDataSourceName).getOrElse {
            sys.error(s"Logical data source '$logicalDataSourceName' not found in data_sources.json.")
          }
          val ddlFile = (dataSourceDef \ "ddlFile").as[String]
          val ddlContent = IO.read(baseDir / ddlFile).replace("TransferTest", tableName)
          
          val createTableCommand = Seq(
            "gcloud", "spanner", "databases", "ddl", "update", databaseId,
            s"--instance=$instanceId",
            s"--project=$projectId",
            s"--ddl=$ddlContent"
          )

          println(s"Executing DDL to create table '$tableName' in database '$databaseId':")
          println(ddlContent)

          if (createTableCommand.! != 0) {
            sys.error(s"Failed to create table '$tableName'.")
          } else {
            println(s"Successfully created table '$tableName'.")
          }
        }
      },

      runDatabricksNotebook := {
      val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
      if (args.isEmpty) {
        sys.error("Usage: sbt \"runDatabricksNotebook <path-to-notebook>\"")
      }
      val notebookPath = args.head
      val baseDir = baseDirectory.value

      val envFile = baseDir / "environment.json"
      if (!envFile.exists()) sys.error(s"Environment file not found at ${envFile.getAbsolutePath}.")
      val environmentConfig = loadBenchmarkConfig(envFile)

      val databricksConfig = (environmentConfig \ "databricks").asOpt[JsObject].getOrElse {
        sys.error("Configuration for environment 'databricks' not found in environment.json.")
      }

      val configWithNotebook = databricksConfig + ("localNotebookFilePath" -> Json.toJson(notebookPath))

      runDatabricksNotebookHelper(configWithNotebook, baseDir)
    },

      refreshDatabricksToken := {
      val baseDir = baseDirectory.value
      val envFile = baseDir / "environment.json"
      if (!envFile.exists()) {
        sys.error(s"Environment file not found at ${envFile.getAbsolutePath}. Please create it from the template.")
      }

      println("Requesting new Databricks token via Databricks CLI...")
      val lifetimeSeconds = 3600 // 1 hour
      val createTokenCommand = Seq(
        "databricks", "tokens", "create",
        "--comment", "Temporary token for Spark Spanner benchmark",
        "--lifetime-seconds", lifetimeSeconds.toString
      )

      val output = try {
        Process(createTokenCommand).!!.trim
      } catch {
        case ex: Exception => sys.error("Failed to create Databricks token. Make sure the Databricks CLI is installed and configured with a valid token that has permission to create new tokens.")
      }

      val tokenJson = Json.parse(output)
      val newToken = (tokenJson \ "token_value").as[String]

      val environmentConfig = loadBenchmarkConfig(envFile).as[JsObject]
      
      val databricksConfig = (environmentConfig \ "databricks").asOpt[JsObject].getOrElse {
        sys.error("'databricks' section not found in environment.json.")
      }
      
      val updatedDatabricksConfig = databricksConfig + ("databricksToken" -> Json.toJson(newToken))
      val updatedEnvironmentConfig = environmentConfig + ("databricks" -> updatedDatabricksConfig)
      
      IO.write(envFile, Json.prettyPrint(updatedEnvironmentConfig))
      println(s"Successfully updated databricksToken in ${envFile.getAbsolutePath}.")
    },
      createResultsBucket := {
        val args: Seq[String] = Def.spaceDelimited("<arg>").parsed
        if (args.isEmpty) {
          sys.error("Usage: sbt \"createResultsBucket <environment>\" (e.g., dataproc or databricks)")
        }
        val environmentName = args(0)
        val baseDir = baseDirectory.value
        
        val envFile = baseDir / "environment.json"
        if (!envFile.exists()) sys.error(s"Environment file not found at ${envFile.getAbsolutePath}.")
        val environmentConfig = loadBenchmarkConfig(envFile)

        val specificEnvConfig = (environmentConfig \ environmentName).asOpt[JsObject].getOrElse {
          sys.error(s"Configuration for environment '$environmentName' not found in environment.json.")
        }

        val resultsBucket = (specificEnvConfig \ "resultsBucket").asOpt[String].getOrElse(sys.error(s"resultsBucket not found in environment.json for the '$environmentName' environment."))
        val projectId = (specificEnvConfig \ "projectId").as[String]
        val location = (specificEnvConfig \ "dataprocRegion").asOpt[String].orElse((specificEnvConfig \ "spannerRegion").asOpt[String]).getOrElse("us-central1")

        val command = Seq(
          "gcloud", "storage", "buckets", "create", s"gs://$resultsBucket",
          s"--project=$projectId",
          s"--location=$location",
          "--uniform-bucket-level-access"
        )

        println(s"Running command: ${command.mkString(" ")}")
        // Don't fail if the bucket already exists
        val exitCode = command.!(ProcessLogger(_ => ()))
        if (exitCode != 0) {
          println(s"Could not create bucket '$resultsBucket'. It may already exist.")
        } else {
          println(s"Successfully created GCS bucket '$resultsBucket'.")
        }
      },
      buildBenchmarkJar := (assembly in ThisProject).value,
  )
}