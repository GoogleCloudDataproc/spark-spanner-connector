# Writing to Spanner from Databricks with the Spark Spanner Connector

This guide provides a step-by-step walkthrough for creating and configuring a Databricks on Google Cloud environment to write data to Google Cloud Spanner using the Spark Spanner Connector.

## Prerequisites

Before you begin, ensure you have the following:

*   A Google Cloud Platform (GCP) account with permissions to create Databricks workspaces and Spanner instances.
*   The `gcloud` command-line tool installed and authenticated to your GCP account.
*   The `databricks` command-line tool installed.

## Summary of Steps

1.  **Create a Databricks on Google Cloud Workspace**: Set up a new, Unity Catalog-enabled Databricks workspace.
2.  **Acquire the Connector JAR**: Download the pre-compiled connector from the project's GitHub page.
3.  **Set up GCP Resources**: Prepare the Spanner database, table, and a service account for access.
4.  **Configure Databricks**: Upload artifacts to a Unity Catalog Volume, store credentials securely, and configure a cluster.
5.  **Run the Notebooks**: Execute the sample notebooks to write data from a Delta Lake table to Spanner.

---

## Step 1: Create a Databricks on Google Cloud Workspace

1.  Navigate to the [Google Cloud Marketplace listing for Databricks](https://console.cloud.google.com/marketplace/product/databricks/databricks).
2.  Click **Subscribe** and follow the prompts to enable the Databricks API.
3.  Once subscribed, click **MANAGE ON PROVIDER**. You will be redirected to the Databricks account console to configure your workspace.
4.  Follow the workspace creation wizard. When prompted, ensure you select a region and enable **Unity Catalog**. This will create a new Unity Catalog metastore for your workspace.
5.  Once the workspace is created, open it and launch the SQL Editor. Follow the prompts to create a personal access token (PAT) and configure the Databricks CLI.
6.  Set your workspace URL as an environment variable:

    ```bash
    export DATABRICKS_HOST="https://<your-workspace-url>.gcp.databricks.com"
    ```

---

## Step 2: Acquire the Connector JAR

Download the pre-compiled Spark Spanner Connector JAR from the official GitHub releases page.

1.  Navigate to the releases page: [https://github.com/GoogleCloudDataproc/spark-spanner-connector/releases](https://github.com/GoogleCloudDataproc/spark-spanner-connector/releases)
2.  Find the version of the connector you wish to use.
3.  From the "Assets" section, download the JAR file that matches your Databricks cluster's Spark version (e.g., `spark-3.3-spanner-..jar` for a cluster with Spark 3.3).

Keep track of this downloaded file, as you will upload it to a Unity Catalog Volume later.

---

## Step 3: Set Up GCP Resources

### 1. Set Environment Variables

Before creating the GCP resources, set the following environment variables in your shell to make the commands easier to copy and execute.

```bash
export GCP_PROJECT_ID="your-gcp-project-id"
export SPANNER_INSTANCE_ID="your-spanner-instance-id"
```

### 2. Create a Spanner Database and Table

```bash
# Create the database
gcloud spanner databases create spark_spanner_db_example \
  --instance=$SPANNER_INSTANCE_ID

# Create the table using the local DDL file
gcloud spanner databases ddl update spark_spanner_db_example \
  --instance=$SPANNER_INSTANCE_ID \
  --ddl-file=./create_source_table.sql
```

### 3. Create and Configure a Service Account

Create a dedicated service account for Databricks and grant it the necessary permissions.

Note the location of the `gcp-credentials.json` file, as it is used in later steps.

```bash
# Create the service account
gcloud iam service-accounts create spanner-databricks-writer \
  --display-name="Databricks Spanner Connector SA"

# Grant permissions
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:spanner-databricks-writer@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/spanner.databaseUser"

# Create and download a JSON key file
gcloud iam service-accounts keys create gcp-credentials.json \
  --iam-account=spanner-databricks-writer@$GCP_PROJECT_ID.iam.gserviceaccount.com
```

---

## Step 4: Configure Your Databricks Workspace

### 1. Create a Unity Catalog Volume

A Unity Catalog Volume is the recommended location for storing workspace artifacts like JARs and init scripts.

```bash
# Set names for Unity Catalog assets
export DATABRICKS_CATALOG="<your-unity-catalog-name>"
export DATABRICKS_SCHEMA="default"
export DATABRICKS_VOLUME="spanner_connector_assets"

# Create the Volume
databricks volumes create \
  $DATABRICKS_CATALOG \
  $DATABRICKS_SCHEMA \
  $DATABRICKS_VOLUME \
  MANAGED
```

### 2. Upload Artifacts to the Volume

Upload the connector JAR (from Step 2) and the `setup_gcp_credentials.sh` init script to the new volume.

```bash
# Define the volume path
export VOLUME_PATH="dbfs:/Volumes/$DATABRICKS_CATALOG/$DATABRICKS_SCHEMA/$DATABRICKS_VOLUME"

# Upload the init script
databricks fs cp ./setup_gcp_credentials.sh ${VOLUME_PATH}/setup_gcp_credentials.sh --overwrite

export CONNECTOR_JAR_PATH="<path-to-connector-jar>"
# Upload the JAR (replace with the actual path to your downloaded JAR)
databricks fs cp ${CONNECTOR_JAR_PATH} ${VOLUME_PATH}/ --overwrite
```

### 3. Allow-list the Artifacts (If Required)

In some secure Unity Catalog environments, JARs and init scripts must be explicitly allow-listed before they can be used.

To edit allowlist in Databricks UI:
1. Navigate to Catalog view, then click the gear button and select Metastore from dropdown.
2. Select **Allowed JARs/Init Scripts** tab on the Metastore screen.
3. Use the **Add** button to add the init script and the connector JAR to the allow-list.

See Databricks [documentation](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/allowlist#how-to-add-items-to-the-allowlist) for more details.

### 4. Store the GCP Key in Databricks Secrets

Create a Databricks secret scope and securely store the content of the `gcp-credentials.json` file.

```bash
databricks secrets create-scope gcp-spanner-secrets
databricks secrets put-secret gcp-spanner-secrets spanner-writer-key < ./gcp-credentials.json
```

### 5. Create and Configure a Databricks Cluster

Create a cluster with the following settings:

1.  **Access Mode**: Choose **Dedicated (formerly: Single User)** and select your user account (or group). This is required for Unity Catalog passthrough.
2.  **Databricks Runtime**: Select a runtime compatible with your connector JAR (e.g., **12.2 LTS** for Spark 3.3).
3.  **Configure Environment Variables**: Under **Advanced Options** > **Spark**, add the following variables.

    ```
    GCP_CREDENTIALS={{secrets/gcp-spanner-secrets/spanner-writer-key}}
    GCP_PROJECT_ID=your-gcp-project-id
    SPANNER_INSTANCE_ID=your-spanner-instance-id
    ```
    Replace the placeholder values with your actual GCP Project ID and Spanner Instance ID.

4.  **Configure Init Script**: Under the **Init Scripts** tab, add an init script with **Source** Volume and the path to the script uploaded earlier.

    ```
    /Volumes/${DATABRICKS_CATALOG}/default/spanner_connector_assets/setup_gcp_credentials.sh
    ```

5.  **Install Library**: Under the **Libraries** tab, install the connector JAR from your Volume.
    *   **Install new** > **Library Source/Volumes**.
    *   Use the path to the JAR in your Volume, e.g., `/Volumes/${DATABRICKS_CATALOG}/default/spanner_connector_assets/<connector-jar-filename>`.

6.  Start (or restart) the cluster.

---

## Step 5: Run the Notebooks

### 1. Import the Notebooks

Import the two provided Python notebooks into your Databricks workspace:
*   `create_delta_lake_table.py`
*   `write_to_spanner.py`
```bash
databricks workspace import /Shared/create_delta_lake_table --file ./create_delta_lake_table.py --format SOURCE --language PYTHON --overwrite
databricks workspace import /Shared/write_to_spanner --file ./write_to_spanner.py --format SOURCE --language PYTHON --overwrite
```
### 2. Run `create_delta_lake_table`

Attach this notebook to your configured cluster and run all cells to create the `sample_data_for_spanner` Delta table.

### 3. Run `write_to_spanner`

Attach this notebook to your cluster and run all cells. The notebook will automatically use the environment variables from the cluster to connect to Spanner and write the data.

Congratulations! You have successfully configured a Unity Catalog-native environment to write data from Databricks to Google Cloud Spanner.
