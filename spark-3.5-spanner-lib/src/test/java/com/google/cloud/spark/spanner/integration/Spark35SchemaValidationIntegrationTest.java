// Copyright 2025 Google LLC
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
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class Spark35SchemaValidationIntegrationTest extends SchemaValidationIntegrationTestBase {
  public Spark35SchemaValidationIntegrationTest(boolean usePostgreSql) {
    super(usePostgreSql);
  }

  @Override
  @Test
  public void testPartialWriteFailsWithoutOption() {
    StructType partialSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.LongType, false)});
    List<Row> rows = Collections.singletonList(RowFactory.create(1L));
    Dataset<Row> df = spark.createDataFrame(rows, partialSchema);

    Map<String, String> props = connectionProperties(usePostgreSql);
    props.put("table", SCHEMA_VALIDATION_TABLE_NAME);
    // "enablePartialRowUpdates" is NOT set

    // Expect Spark's AnalysisException, not our custom SpannerConnectorException
    AnalysisException e =
        assertThrows(
            AnalysisException.class,
            () -> df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save());

    String message = e.getMessage();
    assertThat(message).contains("Cannot write incompatible data for the table");
    assertThat(message).contains(SCHEMA_VALIDATION_TABLE_NAME);
    assertThat(message).contains("Cannot find data for the output column");
    assertThat(message).contains("name");
  }
}
