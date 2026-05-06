# Copy the *.tbl files generated from dbgen here and run
```azure
./convert.sh
```

# *.csv files will be produced that can be uploaded to object storage.

Create a GCS bucket and upload the .csv files.

```
gcloud storage buckets create gs://my-tpch-data
gcloud storage cp *.csv gs://my-tpch-data/
gcloud storage cp manifest.json gs://my-tpch-data/
```

#Import Data Using Dataflow
Use the Dataflow CSV to Spanner template.

```
gcloud dataflow jobs run steve-upload-tpch \
--gcs-location gs://dataflow-templates/latest/GCS_Text_to_Cloud_Spanner \
--region us-central1 \
--parameters instanceId=slord-spark-dev,databaseId=test-tpch,importManifest=gs://my-tpch-data/manifest.json,columnDelimiter="|"
```
Provide the GCS path to your files, the Spanner instance/database, and the CSV file definitions (mapping files to tables).