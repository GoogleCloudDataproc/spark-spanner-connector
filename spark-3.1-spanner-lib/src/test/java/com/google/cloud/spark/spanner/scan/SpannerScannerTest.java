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

package com.google.cloud.spark.spanner.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spark.spanner.planning.query.DirectSqlQueryPlan;
import com.google.cloud.spark.spanner.planning.query.ExecutableQuery;
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.planning.query.LogicalQueryPlan;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;

public class SpannerScannerTest {

  @Test
  public void configuredReadTimestampIsUsed() {
    Timestamp timestamp = Timestamp.parseTimestamp("2026-07-14T12:34:56.123456Z");
    Map<String, String> options = new HashMap<>();
    options.put("readTimestamp", timestamp.toString());

    TimestampBound timestampBound =
        SpannerScanner.getReadTimestamp(new CaseInsensitiveStringMap(options));

    assertEquals(timestamp, timestampBound.getReadTimestamp());
  }

  @Test
  public void readTimestampOptionIsCaseInsensitive() {
    Timestamp timestamp = Timestamp.parseTimestamp("2026-07-14T12:34:56Z");
    Map<String, String> options = new HashMap<>();
    options.put("readtimestamp", timestamp.toString());

    TimestampBound timestampBound =
        SpannerScanner.getReadTimestamp(new CaseInsensitiveStringMap(options));

    assertEquals(timestamp, timestampBound.getReadTimestamp());
  }

  @Test
  public void logicalQueryPlanPreservesTableOptionsProjectionAndRendering() {
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(Collections.emptyMap());
    StructType tableSchema =
        new StructType().add("id", DataTypes.LongType).add("name", DataTypes.StringType);
    SpannerTable spannerTable = mock(SpannerTable.class);
    when(spannerTable.properties()).thenReturn(options);
    when(spannerTable.schema()).thenReturn(tableSchema);
    when(spannerTable.name()).thenReturn("Users");
    LogicalQuery logicalQuery =
        new LogicalQuery(
            spannerTable, Collections.singleton("id"), new Filter[0], Collections.emptyMap());

    ExecutableQuery executableQuery = new LogicalQueryPlan(logicalQuery);

    assertSame(options, executableQuery.getOptions());
    assertEquals(new StructType().add("id", DataTypes.LongType), executableQuery.getReadSchema());
    assertEquals(
        "SELECT `Users`.`id` FROM `Users`",
        executableQuery.buildStatement(Dialect.GOOGLE_STANDARD_SQL).getSql());
    assertEquals(executableQuery.getReadSchema(), new SpannerScanner(executableQuery).readSchema());
  }

  @Test
  public void directSqlQueryPlanPreservesOptionsSchemaAndStatement() {
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(Collections.emptyMap());
    StructType schema = new StructType().add("amount", DataTypes.LongType);
    Statement statement = Statement.of("SELECT A AS amount FROM ATable WHERE A >= 10");

    ExecutableQuery executableQuery = new DirectSqlQueryPlan(options, schema, statement);

    assertSame(options, executableQuery.getOptions());
    assertSame(schema, executableQuery.getReadSchema());
    assertSame(statement, executableQuery.buildStatement(Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(schema, new SpannerScanner(executableQuery).readSchema());
  }
}
