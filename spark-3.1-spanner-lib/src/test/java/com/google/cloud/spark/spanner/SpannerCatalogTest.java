// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spark.spanner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Parameterized.class)
public class SpannerCatalogTest {

  @Parameters
  public static Collection<Dialect> dialects() {
    return Arrays.asList(Dialect.GOOGLE_STANDARD_SQL, Dialect.POSTGRESQL);
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  private SpannerCatalog catalog;
  private final Dialect dialect;

  @Mock private Spanner spanner;
  @Mock private DatabaseClient dbClient;
  @Mock private SpannerInformationSchema spannerInfoSchema;
  @Mock private ReadContext singleUseReadContext;

  public SpannerCatalogTest(Dialect dialect) {
    this.dialect = dialect;
  }

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    catalog =
        new SpannerCatalog() {
          @Override
          protected Spanner createSpanner(CaseInsensitiveStringMap options) {
            return spanner;
          }

          @Override
          protected SpannerInformationSchema createSchemaInfo(Dialect dialect) {
            return spannerInfoSchema;
          }

          @Override
          protected SpannerTable factorySpannerTable(Identifier ident) {
            SpannerTable mockSpannerTable = mock(SpannerTable.class);
            when(mockSpannerTable.name()).thenReturn(ident.name());
            return mockSpannerTable;
          }
        };

    Map<String, String> opts = new HashMap<>();
    opts.put("projectId", "p");
    opts.put("instanceId", "i");
    opts.put("databaseId", "d");
    opts.put("emulatorHost", "localhost:9010");
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(opts);
    catalog.initialize("test-catalog", options);

    when(spanner.getDatabaseClient(any(DatabaseId.class))).thenReturn(dbClient);
    when(dbClient.getDialect()).thenReturn(dialect);
    ReadOnlyTransaction mockRoTransaction = mock(ReadOnlyTransaction.class);
    when(dbClient.readOnlyTransaction()).thenReturn(mockRoTransaction);
    when(dbClient.singleUse()).thenReturn(singleUseReadContext);
  }

  @Test
  public void testName() {
    assertEquals("test-catalog", catalog.name());
  }

  @Test
  public void listTablesShouldReturnTables() {
    String[] namespace = new String[0];
    Identifier[] expectedTables = {Identifier.of(namespace, "t1"), Identifier.of(namespace, "t2")};
    when(spannerInfoSchema.listTables(any(ReadContext.class), any(String[].class)))
        .thenReturn(expectedTables);

    Identifier[] tables = catalog.listTables(namespace);
    verify(spannerInfoSchema).listTables(any(ReadContext.class), eq(namespace));
    assertArrayEquals(expectedTables, tables);
  }

  @Test
  public void listTablesShouldReturnEmptyForInvalidNamespace() {
    String[] namespace = new String[] {"p", "i"};
    Identifier[] tables = catalog.listTables(namespace);
    assertEquals(0, tables.length);
  }

  @Test
  public void loadTableShouldThrowNoSuchTableException() throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[0], "non_existent");
    thrown.expect(NoSuchTableException.class);
    catalog.loadTable(ident);
  }

  @Test
  public void loadTableShouldReturnSpannerTable() throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[0], "t1");
    Statement mockStatement = Statement.of("");
    when(spannerInfoSchema.tableExistsStatement("t1")).thenReturn(mockStatement);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(singleUseReadContext.executeQuery(mockStatement)).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getLong(0)).thenReturn(1L);
    Table table = catalog.loadTable(ident);
    assertNotNull(table);
    assertTrue(table instanceof SpannerTable);
    assertEquals("t1", table.name());
  }

  @Test
  public void loadTableShouldThrowExceptionForInvalidNamespace() throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[] {"p", "i"}, "t1");
    thrown.expect(SpannerConnectorException.class);
    catalog.loadTable(ident);
  }

  @Test
  public void tableExistsShouldReturnTrue() {
    Identifier ident = Identifier.of(new String[0], "t1");
    Statement mockStatement = Statement.of("");
    when(spannerInfoSchema.tableExistsStatement(any(String.class))).thenReturn(mockStatement);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(singleUseReadContext.executeQuery(mockStatement)).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getLong(0)).thenReturn(1L);
    assertTrue(catalog.tableExists(ident));
  }

  @Test
  public void tableExistsShouldReturnFalse() {
    Identifier ident = Identifier.of(new String[] {"p", "i", "d"}, "non_existent");
    assertFalse(catalog.tableExists(ident));
  }

  @Test
  public void tableExistsShouldReturnFalseForInvalidNamespace() {
    Identifier ident = Identifier.of(new String[] {"p", "i"}, "t1");
    assertFalse(catalog.tableExists(ident));
  }

  @Test
  public void createTableShouldThrowExceptionOnNoPrimaryKey() {
    Identifier ident = Identifier.of(new String[] {"p", "i", "d"}, "no_pk_table");
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, Metadata.empty()),
              new StructField("name", DataTypes.StringType, true, Metadata.empty())
            });

    thrown.expect(SpannerConnectorException.class);
    thrown.expectMessage(
        "No primary key found for table no_pk_table. Please specify at least one primary key column.");

    SpannerInformationSchema.create(dialect).toDdl(ident, schema);
  }

  @Test
  public void isGraphIdentifierShouldReturnTrue() {
    Identifier ident =
        Identifier.of(new String[0], SpannerCatalog.GRAPH_IDENTIFIER_PREFIX + "{\"graph\":\"G\"}");
    assertTrue(SpannerCatalog.isGraphIdentifier(ident));
  }

  @Test
  public void isGraphIdentifierShouldReturnFalse() {
    Identifier ident = Identifier.of(new String[0], "my_table");
    assertFalse(SpannerCatalog.isGraphIdentifier(ident));
  }

  @Test
  public void loadTableShouldRejectGraphIdentifierWithNamespace() throws NoSuchTableException {
    Identifier ident =
        Identifier.of(
            new String[] {"ns"}, SpannerCatalog.GRAPH_IDENTIFIER_PREFIX + "{\"graph\":\"G\"}");
    thrown.expect(SpannerConnectorException.class);
    catalog.loadTable(ident);
  }

  @Test
  public void loadTableShouldRejectEmptyGraphIdentifier() throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[0], SpannerCatalog.GRAPH_IDENTIFIER_PREFIX);
    thrown.expect(SpannerConnectorException.class);
    thrown.expectMessage("Graph identifier has no encoded properties");
    catalog.loadTable(ident);
  }

  @Test
  public void loadTableShouldRejectMalformedGraphJson() throws NoSuchTableException {
    Identifier ident =
        Identifier.of(new String[0], SpannerCatalog.GRAPH_IDENTIFIER_PREFIX + "not-valid-json");
    thrown.expect(SpannerConnectorException.class);
    thrown.expectMessage("Malformed graph identifier JSON");
    catalog.loadTable(ident);
  }

  @Test
  public void alterTableShouldThrowException() {
    thrown.expect(UnsupportedOperationException.class);
    catalog.alterTable(null, (org.apache.spark.sql.connector.catalog.TableChange[]) null);
  }

  @Test
  public void renameTableShouldThrowException() {
    thrown.expect(UnsupportedOperationException.class);
    catalog.renameTable(null, null);
  }

  @Test
  public void testToDdl() {
    Identifier ident = Identifier.of(new String[] {"p", "i", "d"}, "my_table");
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, SpannerCatalog.PRIMARY_KEY_METADATA),
              new StructField(
                  "id2", DataTypes.StringType, false, SpannerCatalog.PRIMARY_KEY_METADATA),
              new StructField("name", DataTypes.StringType, true, Metadata.empty()),
              new StructField("active", DataTypes.BooleanType, false, Metadata.empty()),
              new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
              new StructField("data", DataTypes.BinaryType, true, Metadata.empty()),
              new StructField("created_at", DataTypes.TimestampType, true, Metadata.empty()),
              new StructField("created_on", DataTypes.DateType, true, Metadata.empty()),
              new StructField("price", DataTypes.createDecimalType(10, 2), true, Metadata.empty()),
            });

    String ddl = SpannerInformationSchema.create(dialect).toDdl(ident, schema);

    if (dialect == Dialect.POSTGRESQL) {
      assertEquals(
          "CREATE TABLE \"my_table\" (\"id\" bigint NOT NULL, \"id2\" varchar NOT NULL, \"name\" varchar, "
              + "\"active\" boolean NOT NULL, \"amount\" float8, \"data\" bytea, "
              + "\"created_at\" timestamptz, \"created_on\" date, \"price\" numeric, "
              + "PRIMARY KEY (\"id\", \"id2\"))",
          ddl);
    } else {
      assertEquals(
          "CREATE TABLE `my_table` (`id` INT64 NOT NULL, `id2` STRING(MAX) NOT NULL, `name` STRING(MAX), "
              + "`active` BOOL NOT NULL, `amount` FLOAT64, `data` BYTES(MAX), `created_at` TIMESTAMP, "
              + "`created_on` DATE, `price` NUMERIC, PRIMARY KEY (`id`, `id2`))",
          ddl);
    }
  }
}
