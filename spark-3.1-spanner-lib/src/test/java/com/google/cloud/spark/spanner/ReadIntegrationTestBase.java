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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class ReadIntegrationTestBase extends SparkSpannerIntegrationTestBase {

  private Dataset<Row> readFromTable(String table) {
    Map<String, String> props = this.connectionProperties();
    return spark
        .read()
        .format("cloud-spanner")
        .option("viewsEnabled", true)
        .option("projectId", props.get("projectId"))
        .option("instanceId", props.get("instanceId"))
        .option("databaseId", props.get("databaseId"))
        .option("emulatorHost", props.get("emulatorHost"))
        .option("table", table)
        .load();
  }

  @Test
  public void testReadFromCompositeTable() {
    Dataset<Row> df = readFromTable("compositeTable");
    long totalRows = df.count();
    assertThat(totalRows).isEqualTo(2);

    // 1. Validate ids.
    List<String> gotIds = df.select("id").as(Encoders.STRING()).collectAsList();
    List<String> expectIds = Arrays.asList("id1", "id2");
    assertThat(gotIds).containsExactlyElementsIn(expectIds);

    // 2. Validate C field string values.
    List<String> gotCs = df.select("C").as(Encoders.STRING()).collectAsList();
    List<String> expectCs = Arrays.asList("foobar", "this one");
    assertThat(gotCs).containsExactlyElementsIn(expectCs);

    // 3. Validate G field boolean values.
    List<Boolean> gotGs = df.select("G").as(Encoders.BOOLEAN()).collectAsList();
    List<Boolean> expectGs = Arrays.asList(true, false);
    assertThat(gotGs).containsExactlyElementsIn(expectGs);
  }

  BigDecimal asSparkBigDecimal(String v) {
    return new BigDecimal(new BigInteger(v), 38, new MathContext(9));
  }
}
