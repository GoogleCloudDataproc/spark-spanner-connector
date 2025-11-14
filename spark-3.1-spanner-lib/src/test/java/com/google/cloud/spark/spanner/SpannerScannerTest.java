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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for SpannerScanner.buildColumnsWithTablePrefix() */
@RunWith(JUnit4.class)
public class SpannerScannerTest {

  @Test
  public void testBuildColumnsWithTablePrefix_googleSql_singleColumn() {
    Set<String> columns = new HashSet<>(Arrays.asList("id"));
    String result = SpannerScanner.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).isEqualTo("`users`.`id`");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_googleSql_multipleColumns() {
    Set<String> columns = new HashSet<>(Arrays.asList("id", "name"));
    String result = SpannerScanner.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).contains("`users`.`id`");
    assertThat(result).contains("`users`.`name`");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_googleSql_columnMatchingTableName() {
    Set<String> columns = new HashSet<>(Arrays.asList("users", "id"));
    String result = SpannerScanner.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).contains("`users`.`users`");
    assertThat(result).contains("`users`.`id`");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_postgreSql_singleColumn() {
    Set<String> columns = new HashSet<>(Arrays.asList("id"));
    String result = SpannerScanner.buildColumnsWithTablePrefix("users", columns, true);
    assertThat(result).isEqualTo("\"users\".\"id\"");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_postgreSql_multipleColumns() {
    Set<String> columns = new HashSet<>(Arrays.asList("id", "name"));
    String result = SpannerScanner.buildColumnsWithTablePrefix("users", columns, true);
    assertThat(result).contains("\"users\".\"id\"");
    assertThat(result).contains("\"users\".\"name\"");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_postgreSql_columnMatchingTableName() {
    Set<String> columns = new HashSet<>(Arrays.asList("users", "id"));
    String result = SpannerScanner.buildColumnsWithTablePrefix("users", columns, true);
    assertThat(result).contains("\"users\".\"users\"");
    assertThat(result).contains("\"users\".\"id\"");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_emptyColumns() {
    Set<String> columns = new HashSet<>();
    String result = SpannerScanner.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).isEmpty();
  }
}
