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

package com.google.cloud.spark.spanner;

public class Spark31SpannerTableProviderTest
    extends SpannerTableProviderBaseTest<Spark31SpannerTableProvider> {

  protected Spark31SpannerTableProvider getInstance() {
    return new Spark31SpannerTableProvider();
  }

  @Test
  public void getTableAllowsLowerCaseProperties() {
    // Arrange
    Spark31SpannerTableProvider provider = new Spark31SpannerTableProvider();
    Map<String, String> props = connectionPropertiesLowerCase(false);

    final StructType partialSchema = new StructType().add("long_col", DataTypes.LongType, false);

    // Act
    Table table = provider.getTable(partialSchema, null, props);
    // Assert
    assertEquals("ATable", table.name());
    // enablePartialRowUpdates test.
    assertEquals(partialSchema, table.schema());
  }
}
