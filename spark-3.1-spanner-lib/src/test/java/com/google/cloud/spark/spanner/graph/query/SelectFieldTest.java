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

package com.google.cloud.spark.spanner.graph.query;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spark.spanner.graph.SpannerGraphConfigs.LabelConfig;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class SelectFieldTest {

  @Test
  public void testSelectFieldQuotesIdentifiersAndPreservesOutputName() {
    SelectField col = SelectField.column("order");
    assertThat(col.outputName).isEqualTo("order");
    assertThat(col.toString()).isEqualTo("`order`");

    SelectField aliasedCol = SelectField.columnWithAlias("src`col", "out col");
    assertThat(aliasedCol.outputName).isEqualTo("out col");
    assertThat(aliasedCol.toString()).isEqualTo("`src``col` AS `out col`");

    SelectField expr = SelectField.expressionWithAlias("CAST(`k` AS STRING)", "id");
    assertThat(expr.outputName).isEqualTo("id");
    assertThat(expr.toString()).isEqualTo("CAST(`k` AS STRING) AS `id`");

    SelectField litNull = SelectField.literal("null");
    assertThat(litNull.toString()).isEqualTo("null");
  }

  @Test
  public void testBuildSelectFieldForIdAndMergeWhereClauses() {
    SelectField singleKey =
        ElementTableQuery.buildSelectFieldForId("internal_id", 3, ImmutableList.of("select"));
    assertThat(singleKey.outputName).isEqualTo("internal_id");
    assertThat(singleKey.toString())
        .isEqualTo("CONCAT(\"3@\", CAST(`select` AS STRING)) AS `internal_id`");

    SelectField multiKey =
        ElementTableQuery.buildSelectFieldForId("internal_id", 7, ImmutableList.of("k`2", "k1"));
    assertThat(multiKey.toString()).contains("CAST(`k1` AS STRING)");
    assertThat(multiKey.toString()).contains("CAST(`k``2` AS STRING)");

    String mergedWhere =
        ElementTableQuery.mergeWhereClauses(
            ImmutableList.of(
                new LabelConfig("Person", ImmutableList.of(), "a = 1 AND b = 2"),
                new LabelConfig("Account", ImmutableList.of(), "c = 3")));
    assertThat(mergedWhere).isEqualTo("(a = 1 AND b = 2) OR (c = 3)");
  }
}
