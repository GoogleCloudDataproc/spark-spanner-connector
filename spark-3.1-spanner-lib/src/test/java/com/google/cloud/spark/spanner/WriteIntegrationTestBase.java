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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import org.apache.spark.SparkException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Test;

public class WriteIntegrationTestBase extends SparkSpannerIntegrationTestBase {

  private static ReadIntegrationTestBase readIntegration = new ReadIntegrationTestBase();
  private Map<String, String> props = this.connectionProperties();

  public DataFrameWriter<Row> writerToTable(Dataset<Row> df, String table) {
    return df.write()
        .format("cloud-spanner")
        .option("viewsEnabled", true)
        .option("projectId", props.get("projectId"))
        .option("instanceId", props.get("instanceId"))
        .option("databaseId", props.get("databaseId"))
        .option("emulatorHost", props.get("emulatorHost"))
        .option("table", table);
  }

  @Test
  public void testWritesToTableFail() {
    String table = "compositeTable";
    Dataset<Row> drf = readIntegration.readFromTable(table);
    SparkException e =
        assertThrows(
            SparkException.class,
            () -> {
              DataFrameWriter<Row> dwf = writerToTable(drf.select("id"), table);
              dwf.saveAsTable(table);
            });
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Table implementation does not support writes: default.compositeTable");
  }
}
