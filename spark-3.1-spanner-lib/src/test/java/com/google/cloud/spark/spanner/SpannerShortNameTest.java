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
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerShortNameTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static class UnversionedProvider extends SparkSpannerTableProviderBase {}

  @Test
  public void targetSparkVersionFromClassName() {
    assertEquals(
        "3.1",
        SpannerShortName.targetSparkVersion(
            "com.google.cloud.spark.spanner.Spark31SpannerTableProvider"));
    assertEquals(
        "4.2",
        SpannerShortName.targetSparkVersion(
            "com.google.cloud.spark.spanner.Spark42SpannerTableProvider"));
    assertEquals(
        "4.10",
        SpannerShortName.targetSparkVersion(
            "com.google.cloud.spark.spanner.Spark410SpannerTableProvider"));
    assertNull(SpannerShortName.targetSparkVersion("com.google.cloud.spark.spanner.DefaultSource"));
    assertNull(SpannerShortName.targetSparkVersion("org.example.Spark41SpannerTableProvider"));
  }

  @Test
  public void targetSparkVersionWalksClassHierarchy() {
    assertEquals("3.1", SpannerShortName.targetSparkVersion(Spark31SpannerTableProvider.class));
    assertEquals("3.1", SpannerShortName.targetSparkVersion(DefaultSource.class));
    assertNull(SpannerShortName.targetSparkVersion(UnversionedProvider.class));
  }

  @Test
  public void majorMinor() {
    assertEquals("4.2", SpannerShortName.majorMinor("4.2.0"));
    assertEquals("4.1", SpannerShortName.majorMinor("4.1.1-SNAPSHOT"));
    assertEquals("3.5", SpannerShortName.majorMinor("3.5"));
    assertNull(SpannerShortName.majorMinor("unknown"));
    assertNull(SpannerShortName.majorMinor(null));
  }

  @Test
  public void matchingProviderKeepsShortName() {
    assertEquals(
        "cloud-spanner", SpannerShortName.choose("4.2", "4.2", ImmutableSet.of("4.1", "4.2")));
  }

  @Test
  public void mismatchedProviderYieldsToMatchingProvider() {
    assertEquals(
        "cloud-spanner-4.1", SpannerShortName.choose("4.1", "4.2", ImmutableSet.of("4.1", "4.2")));
    assertEquals(
        "cloud-spanner-4.0", SpannerShortName.choose("4.0", "4.2", ImmutableSet.of("4.0", "4.2")));
    assertEquals(
        "cloud-spanner-3.3", SpannerShortName.choose("3.3", "3.5", ImmutableSet.of("3.3", "3.5")));
  }

  @Test
  public void mismatchedProviderKeepsShortNameWithoutMatchingProvider() {
    // A single jar on a Spark version without its own artifact, e.g. the 3.3 jar on Spark 3.4.
    assertEquals("cloud-spanner", SpannerShortName.choose("3.3", "3.4", ImmutableSet.of("3.3")));
    assertEquals(
        "cloud-spanner", SpannerShortName.choose("4.1", "4.2", Collections.<String>emptySet()));
  }

  @Test
  public void unknownVersionsKeepShortName() {
    assertEquals("cloud-spanner", SpannerShortName.choose(null, "4.2", ImmutableSet.of("4.2")));
    assertEquals("cloud-spanner", SpannerShortName.choose("4.1", null, ImmutableSet.of("4.2")));
  }

  @Test
  public void unversionedProviderKeepsShortName() {
    assertEquals("cloud-spanner", new UnversionedProvider().shortName());
  }

  @Test
  public void providerMatchingRunningSparkKeepsShortName() {
    String running = SpannerShortName.majorMinor(org.apache.spark.package$.MODULE$.SPARK_VERSION());
    // This module tests against the Spark version its provider targets.
    assertEquals("3.1", running);
    assertEquals("cloud-spanner", new Spark31SpannerTableProvider().shortName());
    assertEquals("cloud-spanner", new DefaultSource().shortName());
  }

  @Test
  public void registeredProviderVersionsReadsAllServicesFiles() throws IOException {
    File jarA =
        servicesDir(
            "a",
            "# comment\n"
                + "com.google.cloud.spark.spanner.Spark42SpannerTableProvider\n"
                + "org.example.OtherSource\n");
    File jarB =
        servicesDir(
            "b", "  com.google.cloud.spark.spanner.Spark41SpannerTableProvider  # trailing\n");
    try (URLClassLoader loader =
        new URLClassLoader(
            new URL[] {jarA.toURI().toURL(), jarB.toURI().toURL()}, /* parent= */ null)) {
      assertEquals(
          ImmutableSet.of("4.1", "4.2"), SpannerShortName.registeredProviderVersions(loader));
    }
  }

  private File servicesDir(String name, String contents) throws IOException {
    File root = tmp.newFolder(name);
    Path services =
        root.toPath().resolve("META-INF/services/org.apache.spark.sql.sources.DataSourceRegister");
    Files.createDirectories(services.getParent());
    Files.write(services, contents.getBytes(StandardCharsets.UTF_8));
    return root;
  }
}
