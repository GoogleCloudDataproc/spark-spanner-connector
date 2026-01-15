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

package com.google.cloud.spark.spanner;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.micrometer.observation.Observation.CheckedRunnable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SpannerTestBase {

  // Since in the teardown we delete the Cloud Spanner database, here we append a random value to
  // the database ID to avoid any cross-pollution between concurrently running tests.
  // Note that a database ID must be 2-30 characters long.
  private static final String databaseId =
      System.getenv("SPANNER_DATABASE_ID") + "-" + new Random().nextInt(10000000);
  private static final String databaseIdPg = databaseId + "-pg";
  private static final String instanceId = System.getenv("SPANNER_INSTANCE_ID");
  private static final String projectId = System.getenv("SPANNER_PROJECT_ID");
  private static final String table = "ATable";
  private static final String tablePg = "composite_table";
  private static final String instanceConfigId = "regional-us-central1";
  private static Spanner spanner;
  protected static String emulatorHost = System.getenv("SPANNER_EMULATOR_HOST");

  private static final Logger log = LoggerFactory.getLogger(SpannerTable.class);

  private static SpannerOptions createSpannerOptions() {
    return emulatorHost != null
        ? SpannerOptions.newBuilder().setProjectId(projectId).setEmulatorHost(emulatorHost).build()
        : SpannerOptions.newBuilder().setProjectId(projectId).build();
  }

  private static synchronized boolean createSpanner() {
    if (spanner != null) {
      return false;
    }

    spanner = createSpannerOptions().getService();

    Runtime.getRuntime().addShutdownHook(new Thread(SpannerTestBase::teardown));
    return true;
  }

  protected BatchClient createBatchClient() {
    return spanner.getBatchClient(DatabaseId.of(projectId, instanceId, databaseId));
  }

  private static void runIgnoringAlreadyExist(CheckedRunnable<Exception> runnable)
      throws Exception {
    try {
      runnable.run();
    } catch (Exception e) {
      if (!e.toString().contains("ALREADY_EXISTS")) {
        throw e;
      }
    }
  }

  private static void createAndPopulateDatabase(
      DatabaseAdminClient databaseAdminClient,
      Dialect dialect,
      String databaseId,
      Iterable<String> ddls,
      Iterable<String> dmls)
      throws Exception {
    if (Dialect.POSTGRESQL.equals(dialect) && emulatorHost != null && !emulatorHost.isEmpty()) {
      // Spanner emulator doesn't support the PostgreSql dialect interface.
      // If the emulator is set. We return immediately here.
      // TODO: Throw an exception here instead of failing silently.
      // Caveat: This populatePgDatabase function is always called regardless of whether PG
      // databases are needed or not. Making this throw an exception (instead of failing silently)
      // will stop all tests from using an emulator. A bigger refactoring of the test setup process
      // will be needed to address this.
      return;
    }

    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        runIgnoringAlreadyExist(
            databaseAdminClient.createDatabase(instanceId, databaseId, ddls)::get);
        break;
      case POSTGRESQL:
        runIgnoringAlreadyExist(
            databaseAdminClient.createDatabase(
                    instanceId,
                    dialect.createDatabaseStatementFor(databaseId),
                    dialect,
                    Collections.emptyList())
                ::get);
        runIgnoringAlreadyExist(
            databaseAdminClient.updateDatabaseDdl(instanceId, databaseId, ddls, null)::get);
        break;
    }

    // Insert data into the database.
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
    databaseClient
        .readWriteTransaction()
        .run(
            txn -> {
              runIgnoringAlreadyExist(
                  () -> dmls.forEach(sql -> txn.executeUpdate(Statement.of(sql))));
              return null;
            });

    // Using a smaller value of 1000 statements
    int maxValuesPerTxn = 1000;
    List<List<Mutation>> partitionedMutations =
        Lists.partition(TestData.shakespearMutations, maxValuesPerTxn);
    for (List<Mutation> mutations : partitionedMutations) {
      databaseClient.write(mutations);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    // Create the Spanner handle.
    if (!createSpanner()) {
      return;
    }

    // Create the instance.
    InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
    InstanceInfo instanceInfo =
        InstanceInfo.newBuilder(InstanceId.of(projectId, instanceId))
            .setInstanceConfigId(InstanceConfigId.of(projectId, instanceConfigId))
            .setNodeCount(1)
            .setDisplayName("SparkSpanner Test")
            .build();
    runIgnoringAlreadyExist(() -> instanceAdminClient.createInstance(instanceInfo).get());

    // Create the database and populate data
    log.info("\033[34mInitializing databases!\033[00m");
    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
    createAndPopulateDatabase(
        databaseAdminClient,
        Dialect.GOOGLE_STANDARD_SQL,
        databaseId,
        Iterables.concat(TestData.initialDDL, TestData.initialDDLGraph),
        Iterables.concat(TestData.initialDML, TestData.initialDMLGraph));
    createAndPopulateDatabase(
        databaseAdminClient,
        Dialect.POSTGRESQL,
        databaseIdPg,
        TestData.initialDDLPg,
        TestData.initialDMLPg);
  }

  private static void cleanupDatabase() {
    log.info("\033[33mCleaning up databases\033[00m");
    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
    databaseAdminClient.dropDatabase(instanceId, databaseId);
    databaseAdminClient.dropDatabase(instanceId, databaseIdPg);
  }

  public static void teardown() {
    cleanupDatabase();
    spanner.close();
  }

  protected static Map<String, String> connectionProperties(boolean usePostgreSql) {
    Map<String, String> props = new HashMap<>();
    if (usePostgreSql) {
      props.put("databaseId", databaseIdPg);
      props.put("table", tablePg);
    } else {
      props.put("databaseId", databaseId);
      props.put("table", table);
    }
    props.put("instanceId", instanceId);
    props.put("projectId", projectId);
    if (emulatorHost != null) {
      props.put("emulatorHost", emulatorHost);
    }
    return props;
  }

  protected static Map<String, String> connectionProperties() {
    return connectionProperties(false);
  }

  static InternalRow makeInternalRow(int A, String B, double C) {
    GenericInternalRow row = new GenericInternalRow(3);
    row.setLong(0, A);
    row.update(1, UTF8String.fromString(B));
    row.setDouble(2, C);
    return row;
  }

  static InternalRow makeATableInternalRow(
      long A, String B, byte[] C, ZonedDateTime D, double E, String[] F, String G) {
    GenericInternalRow row = new GenericInternalRow(7);
    row.setLong(0, A);
    row.update(1, UTF8String.fromString(B));
    if (C == null) {
      row.update(2, null);
    } else {
      row.update(2, new GenericArrayData(C));
    }
    row.update(3, SpannerUtils.zonedDateTimeToSparkTimestamp(D));
    SpannerUtils.toSparkDecimal(row, new java.math.BigDecimal(E), 4);

    if (F == null) {
      row.update(5, null);
    } else {
      List<UTF8String> fDest = new ArrayList<>(F.length);
      for (String s : F) {
        fDest.add(UTF8String.fromString(s));
      }
      row.update(5, fDest);
    }
    row.update(6, G == null ? null : UTF8String.fromString(G));
    return row;
  }

  static class InternalRowComparator implements Comparator<InternalRow> {

    @Override
    public int compare(InternalRow r1, InternalRow r2) {
      return r1.toString().compareTo(r2.toString());
    }
  }

  public static InternalRow makeCompositeTableRow(
      String id,
      long[] A,
      String[] B,
      String C,
      java.math.BigDecimal D,
      ZonedDateTime E,
      ZonedDateTime F,
      Boolean G,
      ZonedDateTime[] H,
      ZonedDateTime[] I,
      String J,
      String K) {
    GenericInternalRow row = new GenericInternalRow(12);
    row.update(0, UTF8String.fromString(id));
    row.update(1, A == null ? A : new GenericArrayData(A));
    row.update(2, B == null ? B : new GenericArrayData(toSparkStrList(B)));
    row.update(3, C == null ? C : UTF8String.fromString(C));
    if (D == null) {
      row.update(4, null);
    } else {
      SpannerUtils.toSparkDecimal(row, D, 4);
    }
    row.update(5, E == null ? E : SpannerUtils.zonedDateTimeToSparkDate(E));
    row.update(6, F == null ? F : SpannerUtils.zonedDateTimeToSparkTimestamp(F));
    if (G == null) {
      row.update(7, null);
    } else {
      row.setBoolean(7, G);
    }
    row.update(
        8, H == null ? null : SpannerTestUtils.zonedDateTimeIterToSparkDates(Arrays.asList(H)));
    row.update(
        9, I == null ? I : SpannerTestUtils.zonedDateTimeIterToSparkTimestamps(Arrays.asList(I)));
    row.update(10, J == null ? J : stringToBytes(J));
    row.update(11, K == null ? K : UTF8String.fromString(K));

    return row;
  }

  public static InternalRow makeCompositeTableRowPg(
      long id,
      String charvCol,
      String textCol,
      String varcharCol,
      Boolean boolCol,
      Boolean booleanCol,
      Long bigintCol,
      Long int8Col,
      Long intCol,
      Double doubleCol,
      Double float8Col,
      String byteCol,
      String dateCol,
      java.math.BigDecimal numericCol,
      java.math.BigDecimal decimalCol,
      String timewithzoneCol,
      String timestampCol,
      String jsonCol) {
    GenericInternalRow row = new GenericInternalRow(18);
    row.setLong(0, id);
    row.update(1, charvCol == null ? null : UTF8String.fromString(charvCol));
    row.update(2, textCol == null ? null : UTF8String.fromString(textCol));
    row.update(3, varcharCol == null ? null : UTF8String.fromString(varcharCol));
    if (boolCol == null) {
      row.update(4, null);
    } else {
      row.setBoolean(4, boolCol);
    }
    if (booleanCol == null) {
      row.update(5, null);
    } else {
      row.setBoolean(5, booleanCol);
    }
    if (bigintCol == null) {
      row.update(6, null);
    } else {
      row.setLong(6, bigintCol);
    }
    if (int8Col == null) {
      row.update(7, null);
    } else {
      row.setLong(7, int8Col);
    }
    if (intCol == null) {
      row.update(8, null);
    } else {
      row.setLong(8, intCol);
    }
    if (doubleCol == null) {
      row.update(9, null);
    } else {
      row.setDouble(9, doubleCol);
    }
    if (float8Col == null) {
      row.update(10, null);
    } else {
      row.setDouble(10, float8Col);
    }
    row.update(11, charvCol == null ? null : UTF8String.fromString(byteCol));
    row.update(
        12,
        dateCol == null
            ? null
            : SpannerUtils.zonedDateTimeToSparkDate(ZonedDateTime.parse(dateCol)));
    if (numericCol == null) {
      row.update(13, null);
    } else {
      SpannerUtils.toSparkDecimal(row, numericCol, 13);
    }
    if (decimalCol == null) {
      row.update(14, null);
    } else {
      SpannerUtils.toSparkDecimal(row, decimalCol, 14);
    }
    row.update(
        15,
        timewithzoneCol == null
            ? null
            : SpannerUtils.zonedDateTimeToSparkTimestamp(ZonedDateTime.parse(timewithzoneCol)));
    row.update(
        16,
        timestampCol == null
            ? null
            : SpannerUtils.zonedDateTimeToSparkTimestamp(ZonedDateTime.parse(timestampCol)));
    row.update(17, jsonCol == null ? null : UTF8String.fromString(jsonCol));
    return row;
  }

  private static UTF8String[] toSparkStrList(String[] strs) {
    List<UTF8String> dest = new ArrayList<>();
    for (String s : strs) {
      dest.add(UTF8String.fromString(s));
    }
    return dest.toArray(new UTF8String[0]);
  }

  private static byte[] stringToBytes(String str) {
    byte[] val = new byte[str.length() / 2];
    for (int i = 0; i < val.length; i++) {
      int index = i * 2;
      int j = Integer.parseInt(str.substring(index, index + 2), 16);
      val[i] = (byte) j;
    }
    return val;
  }
}
