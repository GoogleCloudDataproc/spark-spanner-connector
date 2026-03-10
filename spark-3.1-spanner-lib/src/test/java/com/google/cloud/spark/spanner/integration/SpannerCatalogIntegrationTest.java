// Copyright 2026 Google LLC
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

package com.google.cloud.spark.spanner.integration;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spark.spanner.SpannerCatalog;
import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerTable;
import com.google.cloud.spark.spanner.TestData;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SpannerCatalogIntegrationTest extends SparkCatalogSpannerIntegrationTestBase {

  private SpannerCatalog catalog;
  private final boolean usePostgresSql;

  @Override
  protected boolean getUsePostgreSql() {
    return usePostgresSql;
  }

  @Parameters
  public static Collection<Object[]> usePostgresSqlValues() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  public SpannerCatalogIntegrationTest(boolean usePostgresSql) {
    super();
    this.usePostgresSql = usePostgresSql;
  }

  @Before
  public void setupCatalog() {
    catalog = new SpannerCatalog();
    catalog.initialize(
        "spanner", new CaseInsensitiveStringMap(connectionProperties(usePostgresSql)));
  }

  @After
  public void teardownCatalog() {
    catalog.close();
  }

  @Test
  public void testListTables() {
    String[] namespace = new String[0];
    Identifier[] tables = catalog.listTables(namespace);
    List<String> tableNames =
        Arrays.stream(tables)
            .map(Identifier::name)
            .map(String::toLowerCase)
            .collect(Collectors.toList());

    assertThat(tableNames)
        .containsAtLeast("schema_test_table", "write_array_test_table", "write_test_table");
  }

  @Test
  public void testLoadTable() throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[0], "schema_test_table");
    Table table = catalog.loadTable(ident);
    assertTrue(table instanceof SpannerTable);
    assertThat(table.name()).isEqualTo("schema_test_table");
    assertThat(table.schema().fields())
        .asList()
        .containsExactly(
            new StructField("id", DataTypes.LongType, false, SpannerCatalog.PRIMARY_KEY_METADATA),
            new StructField("name", DataTypes.StringType, true, Metadata.empty()),
            new StructField("value", DataTypes.DoubleType, true, Metadata.empty()));
  }

  @Test
  public void testLoadTableNotExists() {
    Identifier ident = Identifier.of(new String[0], "NonExistentTable");
    assertThrows(NoSuchTableException.class, () -> catalog.loadTable(ident));
  }

  @Test
  public void testCreateTableAlreadyExists() {
    Identifier ident = Identifier.of(new String[0], "write_test_table");
    assertThrows(
        SpannerConnectorException.class,
        () -> catalog.createTable(ident, new StructType(), null, new HashMap<>()));
  }

  @Test
  public void testTableExists() {
    Identifier ident = Identifier.of(new String[0], "write_test_table");
    assertTrue(catalog.tableExists(ident));
  }

  @Test
  public void testCreateTable() throws NoSuchTableException {
    String tableName = "new_test_table";
    Identifier ident = Identifier.of(new String[0], tableName);
    StructType createSchema =
        new StructType()
            .add("id", DataTypes.LongType, false, SpannerCatalog.PRIMARY_KEY_METADATA)
            .add("name", DataTypes.StringType, true);
    Map<String, String> properties = new HashMap<>();

    try {
      catalog.createTable(ident, createSchema, null, properties);
      assertTrue(catalog.tableExists(ident));
      Table loadedTable = catalog.loadTable(ident);
      // Connector currently does not retrieve primary key metadata.
      StructType expectedSchema =
          new StructType()
              .add("id", DataTypes.LongType, false, SpannerCatalog.PRIMARY_KEY_METADATA)
              .add("name", DataTypes.StringType, true);
      assertThat(loadedTable.schema()).isEqualTo(expectedSchema);
    } finally {
      catalog.dropTable(ident);
      assertFalse(catalog.tableExists(ident));
    }
  }

  @Test
  public void testTableExistsReturnsFalse() {
    Identifier ident = Identifier.of(new String[0], "non_existent_table");
    assertFalse(catalog.tableExists(ident));
  }

  @Test
  public void testLoadTableRejectsNonEmptyNamespace() {
    Identifier ident = Identifier.of(new String[] {"ns"}, "schema_test_table");
    assertThrows(SpannerConnectorException.class, () -> catalog.loadTable(ident));
  }

  @Test
  public void testListTablesWithInvalidNamespace() {
    Identifier[] tables = catalog.listTables(new String[] {"invalid", "namespace"});
    assertEquals(0, tables.length);
  }

  @Test
  public void testDropTableNonExistent() {
    Identifier ident = Identifier.of(new String[0], "table_that_does_not_exist");
    assertFalse(catalog.dropTable(ident));
  }

  @Test
  public void testIsGraphIdentifier() {
    Identifier graphIdent =
        Identifier.of(new String[0], SpannerCatalog.GRAPH_IDENTIFIER_PREFIX + "{\"graph\":\"G\"}");
    assertTrue(SpannerCatalog.isGraphIdentifier(graphIdent));

    Identifier tableIdent = Identifier.of(new String[0], "my_table");
    assertFalse(SpannerCatalog.isGraphIdentifier(tableIdent));
  }

  @Test
  public void testReadTableViaCatalogSql() {
    if (usePostgresSql) {
      return;
    }
    Dataset<Row> df = spark.sql("SELECT * FROM spanner.simpleTable");
    assertThat(df.count()).isGreaterThan(0);
    assertThat(df.columns()).asList().containsExactly("A", "B", "C");
  }

  @Test
  public void testOverwriteRecreateMode() {
    testOverwriteImpl("recreate");
  }

  @Test
  public void testOverwriteTruncateMode() {
    testOverwriteImpl("truncate");
  }

  private void testOverwriteImpl(String mode) {
    final StructType SIMPLE_SCHEMA =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "long_col", DataTypes.LongType, false, SpannerCatalog.PRIMARY_KEY_METADATA),
              DataTypes.createStructField("string_col", DataTypes.StringType, true),
            });

    String tableName = TestData.WRITE_TABLE_NAME + "_" + mode;
    String qualifiedTableName = "spanner." + tableName;
    spark.sql("DROP TABLE IF EXISTS " + qualifiedTableName);

    // 1. Define schema with primary key metadata (needed for table recreation)

    Map<String, String> props = connectionProperties(usePostgresSql);
    props.put("table", tableName);
    props.put("overwriteMode", mode);

    // 2. Write initial data (creates the table via ErrorIfExists)
    List<Row> initialRows =
        Arrays.asList(RowFactory.create(1L, "initial-one"), RowFactory.create(2L, "initial-two"));
    Dataset<Row> initialDf = spark.createDataFrame(initialRows, SIMPLE_SCHEMA);
    initialDf
        .write()
        .format("cloud-spanner")
        .options(props)
        .mode(SaveMode.ErrorIfExists)
        .saveAsTable(qualifiedTableName);

    // 3. Verify initial data
    Dataset<Row> dfAfterInitialWrite =
        spark.read().format("cloud-spanner").options(props).table(qualifiedTableName);
    assertEquals(2, dfAfterInitialWrite.count());

    // 4. Overwrite with recreate mode
    List<Row> newRows =
        Arrays.asList(
            RowFactory.create(3L, "new-three"),
            RowFactory.create(4L, "new-four"),
            RowFactory.create(5L, "new-five"));
    Dataset<Row> newDf = spark.createDataFrame(newRows, SIMPLE_SCHEMA);

    newDf
        .write()
        .format("cloud-spanner")
        .options(props)
        .mode(SaveMode.Overwrite)
        .save(qualifiedTableName);

    // 5. Verify only new data exists
    Dataset<Row> finalDf =
        spark.read().format("cloud-spanner").options(props).load(qualifiedTableName);
    assertEquals(3, finalDf.count());

    Map<Long, Row> finalRows =
        finalDf.collectAsList().stream()
            .collect(java.util.stream.Collectors.toMap(r -> r.getLong(0), r -> r));

    assertThat(finalRows.get(3L).getString(1)).isEqualTo("new-three");
    assertThat(finalRows.get(4L).getString(1)).isEqualTo("new-four");
    assertThat(finalRows.get(5L).getString(1)).isEqualTo("new-five");
  }
}
