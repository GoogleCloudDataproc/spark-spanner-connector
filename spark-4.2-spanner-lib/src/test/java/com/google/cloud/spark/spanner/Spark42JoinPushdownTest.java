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

package com.google.cloud.spark.spanner;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spark.spanner.scan.InterleaveTableMetadata;
import com.google.cloud.spark.spanner.scan.SpannerTable;
import com.google.cloud.spark.spanner.scan.Spark41SpannerScanBuilder;
import java.util.Map;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.join.JoinType;
import org.apache.spark.sql.connector.read.SupportsPushDownJoin.ColumnWithAlias;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;

public class Spark42JoinPushdownTest {

  @Test
  public void pushDownJoinWorksWithSpark42DsV2Apis() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, Metadata.empty()),
              new StructField("name", DataTypes.StringType, true, Metadata.empty())
            });
    Map<String, String> spannerTypes = Map.of("id", "INT64", "name", "STRING");
    CaseInsensitiveStringMap props =
        new CaseInsensitiveStringMap(Map.of("enablePredicateSql", "true"));

    SpannerTable parentTable = mock(SpannerTable.class);
    InterleaveTableMetadata parentMeta = mock(InterleaveTableMetadata.class);
    when(parentMeta.getTableName()).thenReturn("Parent");
    when(parentMeta.getParentTable()).thenReturn(null);
    when(parentTable.name()).thenReturn("Parent");
    when(parentTable.getProjectId()).thenReturn("proj");
    when(parentTable.getInstanceId()).thenReturn("inst");
    when(parentTable.getDatabaseId()).thenReturn("db");
    when(parentTable.schema()).thenReturn(schema);
    when(parentTable.properties()).thenReturn(props);
    when(parentTable.getSpannerTypes()).thenReturn(spannerTypes);
    when(parentTable.getInterleavedTableMetadata()).thenReturn(parentMeta);

    SpannerTable childTable = mock(SpannerTable.class);
    InterleaveTableMetadata childMeta = mock(InterleaveTableMetadata.class);
    when(childMeta.getTableName()).thenReturn("Child");
    when(childMeta.getParentTable()).thenReturn("Parent");
    when(childTable.name()).thenReturn("Child");
    when(childTable.getProjectId()).thenReturn("proj");
    when(childTable.getInstanceId()).thenReturn("inst");
    when(childTable.getDatabaseId()).thenReturn("db");
    when(childTable.schema()).thenReturn(schema);
    when(childTable.properties()).thenReturn(props);
    when(childTable.getSpannerTypes()).thenReturn(spannerTypes);
    when(childTable.getInterleavedTableMetadata()).thenReturn(childMeta);

    Spark41SpannerScanBuilder left = new Spark41SpannerScanBuilder(parentTable);
    Spark41SpannerScanBuilder right = new Spark41SpannerScanBuilder(childTable);

    assertTrue(left.isOtherSideCompatibleForJoin(right));

    ColumnWithAlias[] leftCols = new ColumnWithAlias[] {new ColumnWithAlias("id", "p_id")};
    ColumnWithAlias[] rightCols = new ColumnWithAlias[] {new ColumnWithAlias("id", "c_id")};
    NamedReference leftRef = FieldReference.column("p_id");
    NamedReference rightRef = FieldReference.column("c_id");
    Predicate predicate = new Predicate("=", new Expression[] {leftRef, rightRef});

    assertTrue(left.pushDownJoin(right, JoinType.INNER_JOIN, leftCols, rightCols, predicate));
  }
}
