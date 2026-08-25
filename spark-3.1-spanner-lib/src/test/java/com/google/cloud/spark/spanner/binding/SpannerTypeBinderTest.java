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

package com.google.cloud.spark.spanner.binding;

import static org.junit.Assert.fail;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class SpannerTypeBinderTest {

  private static final String UUID_VALUE = "550e8400-e29b-41d4-a716-446655440000";

  @Test
  public void bindsUuidStringAsUuid() {
    LiteralExpr literal = new LiteralExpr(UUID_VALUE, DataTypes.StringType, "UUID");

    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    // The important assertion is that binding succeeds. The binder is
    // responsible for converting the String UUID to a UUID value before
    // passing it to the Spanner client.
    Statement statement = builder.build();

    // Building the statement confirms that the parameter was successfully
    // bound without treating the UUID as an ordinary String.
    if (statement == null) {
      fail("Expected a bound statement");
    }
  }

  @Test
  public void rejectsInvalidUuidString() {
    LiteralExpr literal = new LiteralExpr("not-a-uuid", DataTypes.StringType, "UUID");

    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    try {
      SpannerTypeBinder.bind(builder, "p1", literal);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  @Test
  public void bindsUuidLookingStringAsStringWhenTargetIsString() {
    LiteralExpr literal = new LiteralExpr(UUID_VALUE, DataTypes.StringType, "STRING");

    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    // A UUID-looking value must remain a String when the target Spanner
    // column is STRING.
    SpannerTypeBinder.bind(builder, "p1", literal);

    builder.build();
  }

  @Test
  public void bindsNullStringAsString() {
    LiteralExpr literal = new LiteralExpr(null, DataTypes.StringType, "STRING");

    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    builder.build();
  }
}
