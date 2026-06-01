# Spark Spanner TPC-H benchmark

This benchmark is designed to test the performance of the Spark Spanner Connector for read operations using a TPC-H dataset. It can be run on Google Cloud Dataproc.

## Getting Started

This guide walks through setting up your Google Cloud environment to run the Spark Spanner Connector benchmarks.

### Prepare the data
#### 1. ConfigureTPC-H benchmark set up

1. Generate TPC-H Data (.tbl files)
Download the official TPC-H toolkit from the TPC website https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-H&bm_vers=3.0.1&mode=CURRENT-ONLY

2. Compile dbgen and run it to generate data.
``` bash
./dbgen -s 1  # -s 1 generates 1GB of data
```
* This generates files like customer.tbl, orders.tbl, etc., which are pipe-delimited (|).

3. Copy the *.tbl files generated from dbgen to the project benchmark/tpch/table directory.
``` bash
cd <project location>/benchmark/tpch/table
./convert.sh
```
* The output will be *.csv files which are equivalent to the *.tbl files.

#### 2. Upload the .csv files to object storage.

1. Create a GCS bucket <my-tpch-data> and upload the .csv files and also the manifest.json file contained in this table directory there.
``` bash
gcloud storage buckets create gs://my-tpch-data
gcloud storage cp *.csv gs://my-tpch-data/
gcloud storage cp manifest.json gs://my-tpch-data/
```

#### 3. Import Data Using Dataflow
1.  Use the Dataflow CSV to Spanner template.
``` bash
gcloud dataflow jobs run steve-upload-tpch \
--gcs-location gs://dataflow-templates/latest/GCS_Text_to_Cloud_Spanner \
--region us-central1 \
--parameters instanceId=slord-spark-dev,databaseId=test-tpch,importManifest=gs://my-tpch-data/manifest.json,columnDelimiter="|"
```
* Provide the GCS path to your files, the Spanner instance/database, and the CSV file definitions (mapping files to tables).
* This creates a test-tpch database in the slord-spark-dev Spanner instance containing the TCP-H tables populated with the generated data.

## Other environment set up

### Dataproc configuration

1. Create a storage bucket which is used when tests are run. eg steve-staging-bucket
2. Create a storage bucket which is used for results, eg steve-benchmark-results.
3. Create a folder in the bucket called answers.

### Create a Dataproc instance on which to run dataproc benchmark tests
1. Create a Dataproc instance on GCP. Search for Managed Service for Apache Spark.
2. Create the cluster
   1. Name the cluster and use this name in environment.json for your tpch environment. 
   2. Under Advanced Configuration - Other, set the Cloud Storage staging bucket <steve-staging-bucket> to your staging bucket. 
   3. Click Create

### Create a Databricks cluster on which to run Databricks benchmark tests
1. In Databricks UI select the workspace where you will run the benchmark tests.
2. In sidebar, select Compute - Create Compute
3. Configure the following:
   1. Performance - Databricks runtime: 16.4 LTS (Scala 2.12) - this is for Spark 3.x benchmarking
   2. Access mode - Manual - Dedicated: Single User
   3. Configure the Cluster Init Script

      The `benchmark/setup_gcp_credentials.sh` script is designed to run as a cluster-scoped init script. It reads the secret you just created and installs it as the Application Default Credentials (ADC) file on each node in the cluster.
      1.  **Upload the init script**: Upload `benchmark/setup_gcp_credentials.sh` to a location on your Databricks workspace or DBFS (e.g., `dbfs:/databricks/init_scripts/setup_gcp_credentials.sh`).
      2.  **Configure the cluster**: In your Databricks cluster configuration, navigate to "Advanced Options" -> "Init Scripts". Add the path of the script you just uploaded.
      3.  **Set Environment Variables**: In the same cluster configuration, under "Advanced Options" -> "Spark", set the following environment variable. This tells the init script which secret to read.
          ```
          GCP_CREDENTIALS={{secrets/gcp-credentials/spanner-benchmark-sa}}
          ```
         Replace `gcp-credentials` and `spanner-benchmark-sa` with the scope and secret name you created in step 1.

         When the cluster starts, the init script will run on every node, creating the file `/root/.config/gcloud/application_default_credentials.json`. The Spark Spanner connector and GCS connector will automatically pick up and use these credentials.

### Creating expected output

* Expected results have to be generated for each query.

1. Run each query in Spanner Studio and export the results to Google Sheets.
2. Download the Google Sheets file as a tab separated file (.tsv) named q<query number>.tsv
3. Convert the .tsv file to a pipe (|) separated file with the extention .out. Eg for query 3
``` bash
sed $'s/\t/|/g'  q3.tsv > q3.out
```
4. Copy expected results to GCP
```bash
gcloud storage cp answers/q3.out gs://steve-benchmark-results/answers
```

## Run dataproc benchmark test
``` bash
sbt "runBenchmark dataproc-tpch-q3"
```

## Databricks benchmark test
``` bash
sbt "runBenchmark databricks-tpch-q1"
```
