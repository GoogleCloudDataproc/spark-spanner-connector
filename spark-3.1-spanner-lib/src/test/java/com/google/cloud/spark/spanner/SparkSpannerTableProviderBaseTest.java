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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SparkSpannerTableProviderBaseTest {

  private static final Gson GSON = new Gson();

  // A test implementation of the abstract SparkSpannerTableProviderBase
  private static class TestSparkSpannerTableProvider extends SparkSpannerTableProviderBase {}

  @Test
  public void testExtractIdentifier() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectId", "p");
                put("instanceId", "i");
                put("databaseId", "d");
                put("table", "t");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    assertEquals(Identifier.of(new String[0], "t"), identifier);
  }

  @Test
  public void testExtractIdentifierWithTableAndGraph() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectId", "p");
                put("instanceId", "i");
                put("databaseId", "d");
                put("table", "t");
                put("graph", "g");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    assertEquals(Identifier.of(new String[0], "t"), identifier);
  }

  @Test
  public void testExtractIdentifierWithNoTable() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectId", "p");
                put("instanceId", "i");
                put("databaseId", "d");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    assertNull(identifier);
  }

  @Test
  public void testExtractIdentifierWithGraphOnly() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("projectId", "p");
                put("instanceId", "i");
                put("databaseId", "d");
                put("graph", "MyGraph");
                put("type", "node");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    assertNotNull(identifier);
    assertTrue(identifier.name().startsWith(SpannerCatalog.GRAPH_IDENTIFIER_PREFIX));
    String json = identifier.name().substring(SpannerCatalog.GRAPH_IDENTIFIER_PREFIX.length());
    Map<String, String> decoded =
        GSON.fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
    assertEquals("MyGraph", decoded.get("graph"));
    assertEquals("node", decoded.get("type"));
    assertEquals(0, identifier.namespace().length);
  }

  @Test
  public void testExtractIdentifierGraphEncodesAllOptions() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("graph", "G");
                put("type", "node");
                put("enableDataBoost", "true");
                put("configs", "{}");
                put("graphQuery", "SELECT 1");
                put("timestamp", "2024-01-01T00:00:00Z");
                put("viewsEnabled", "true");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    String json = identifier.name().substring(SpannerCatalog.GRAPH_IDENTIFIER_PREFIX.length());
    Map<String, String> decoded =
        GSON.fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
    assertEquals(7, decoded.size());
    assertEquals("G", decoded.get("graph"));
    assertEquals("true", decoded.get("enableDataBoost"));
    assertEquals("{}", decoded.get("configs"));
    assertEquals("SELECT 1", decoded.get("graphQuery"));
    assertEquals("2024-01-01T00:00:00Z", decoded.get("timestamp"));
    assertEquals("true", decoded.get("viewsEnabled"));
  }

  @Test
  public void testExtractIdentifierGraphEncodesOnlyPresentOptions() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("graph", "G");
                put("projectId", "p");
              }
            });
    Identifier identifier = provider.extractIdentifier(options);
    String json = identifier.name().substring(SpannerCatalog.GRAPH_IDENTIFIER_PREFIX.length());
    Map<String, String> decoded =
        GSON.fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
    assertEquals(1, decoded.size());
    assertEquals("G", decoded.get("graph"));
    assertNull(decoded.get("projectId"));
  }

  @Test
  public void testShortName() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    assertEquals("cloud-spanner", provider.shortName());
  }

  @Test
  public void testSupportsExternalMetadata() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    assertTrue(provider.supportsExternalMetadata());
  }

  @Test
  public void testExtractCatalog() {
    TestSparkSpannerTableProvider provider = new TestSparkSpannerTableProvider();
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new HashMap<String, String>() {
              {
                put("catalog", "cat");
              }
            });
    assertEquals("cat", provider.extractCatalog(options));
    CaseInsensitiveStringMap options2 = new CaseInsensitiveStringMap(new HashMap<>());
    assertEquals("spanner", provider.extractCatalog(options2));
  }
}
