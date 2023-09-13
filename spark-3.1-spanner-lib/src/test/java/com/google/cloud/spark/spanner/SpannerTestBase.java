// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spark;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import com.google.spanner.admin.instance.v1.InstanceName;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;

class SpannerTestBase {
  // TODO: Generate dynamic database name to avoid conflicts.
  private static String databaseId = System.getenv("SPANNER_DATABASE_ID");
  private static String instanceId = System.getenv("SPANNER_INSTANCE_ID");
  private static String projectId = System.getenv("SPANNER_PROJECT_ID");
  private static String emulatorHost = System.getenv("SPANNER_EMULATOR_HOST");
  private static String table = "ATable";
  private static Spanner spanner = createSpanner();

  private static SpannerOptions createSpannerOptions() {
    return emulatorHost != null
        ? SpannerOptions.newBuilder().setProjectId(projectId).setEmulatorHost(emulatorHost).build()
        : SpannerOptions.newBuilder().setProjectId(projectId).build();
  }

  private static Spanner createSpanner() {
    return createSpannerOptions().getService();
  }

  protected BatchClient createBatchClient() {
    return spanner.getBatchClient(DatabaseId.of(projectId, instanceId, databaseId));
  }

  private static void initDatabase() throws Exception {
    // 1. Create the Spanner instance.
    InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
    InstanceConfig config =
        instanceAdminClient.listInstanceConfigs().iterateAll().iterator().next();
    InstanceInfo instanceInfo =
        InstanceInfo.newBuilder(InstanceId.of(projectId, instanceId))
            .setInstanceConfigId(config.getId())
            .setNodeCount(1)
            .setDisplayName("SparkSpanner Test")
            .build();
    OperationFuture<Instance, CreateInstanceMetadata> createInstanceOperation =
        instanceAdminClient.createInstance(instanceInfo);

    try {
      createInstanceOperation.get();
    } catch (Exception e) {
      if (!e.toString().contains("ALREADY_EXISTS")) {
        throw e;
      }
    }

    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();

    // 2. Create the database.
    // TODO: Skip this process if the database already exists.
    OperationFuture<Database, CreateDatabaseMetadata> createDatabaseOperation =
        databaseAdminClient.createDatabase(instanceId, databaseId, TestData.initialDDL);
    try {
      createDatabaseOperation.get();
    } catch (Exception e) {
      if (!e.toString().contains("ALREADY_EXISTS")) {
        throw e;
      }
    }

    // 3. Insert data into the databse.
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
    databaseClient
        .readWriteTransaction()
        .run(
            txn -> {
              try {
                TestData.initialDML.forEach(sql -> txn.executeUpdate(Statement.of(sql)));
              } catch (Exception e) {
                if (!e.toString().contains("ALREADY_EXISTS")) {
                  throw e;
                }
              }

              return null;
            });
  }

  @BeforeClass
  public static void setUp() throws Exception {
    initDatabase();
  }

  private static void cleanupInstance() {
    InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
    InstanceName name = InstanceName.of(projectId, instanceId);
    instanceAdminClient.deleteInstance(name.getInstance());
  }

  @AfterClass
  public static void teardown() {
    cleanupInstance();
    spanner.close();
  }

  protected static Map<String, String> connectionProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("databaseId", databaseId);
    props.put("instanceId", instanceId);
    props.put("projectId", projectId);
    if (emulatorHost != null) {
      props.put("emulatorHost", emulatorHost);
    }
    props.put("table", table);
    return props;
  }
}
