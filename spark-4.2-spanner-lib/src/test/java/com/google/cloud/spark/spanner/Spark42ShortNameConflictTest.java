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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Simulates the Spark 4.2 connector bundled with Spark plus an older connector jar passed with
 * {@code --jars}: the older jar's services file registers its own provider class, which resolves to
 * the bundled copy of that class.
 */
@RunWith(JUnit4.class)
public class Spark42ShortNameConflictTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void olderProvidersYieldShortNameOnSpark42() {
    assertEquals("cloud-spanner", new Spark42SpannerTableProvider().shortName());
    assertEquals("cloud-spanner", new DefaultSource().shortName());
    withExtraRegistrations(
        () -> {
          assertEquals("cloud-spanner-4.1", new Spark41SpannerTableProvider().shortName());
          assertEquals("cloud-spanner-4.0", new Spark40SpannerTableProvider().shortName());
          assertEquals("cloud-spanner-3.5", new Spark35SpannerTableProvider().shortName());
          assertEquals("cloud-spanner", new Spark42SpannerTableProvider().shortName());
        },
        "com.google.cloud.spark.spanner.Spark41SpannerTableProvider",
        "com.google.cloud.spark.spanner.Spark40SpannerTableProvider",
        "com.google.cloud.spark.spanner.Spark35SpannerTableProvider");
  }

  @Test
  public void lookupDataSourceResolvesSpark42WithOlderJarsRegistered() {
    withExtraRegistrations(
        () ->
            assertEquals(
                Spark42SpannerTableProvider.class,
                DataSource.lookupDataSource("cloud-spanner", SQLConf.get())),
        "com.google.cloud.spark.spanner.Spark41SpannerTableProvider",
        "com.google.cloud.spark.spanner.Spark40SpannerTableProvider");
  }

  @Test
  public void versionQualifiedNameSelectsOlderProvider() {
    withExtraRegistrations(
        () ->
            assertEquals(
                Spark41SpannerTableProvider.class,
                DataSource.lookupDataSource("cloud-spanner-4.1", SQLConf.get())),
        "com.google.cloud.spark.spanner.Spark41SpannerTableProvider");
  }

  /**
   * Runs {@code body} with a context class loader that adds a services file listing {@code
   * providers}, on top of this module's own registration of Spark42SpannerTableProvider.
   */
  private void withExtraRegistrations(Runnable body, String... providers) {
    Thread thread = Thread.currentThread();
    ClassLoader original = thread.getContextClassLoader();
    try (URLClassLoader loader =
        new URLClassLoader(new URL[] {servicesDir(providers).toURI().toURL()}, original)) {
      thread.setContextClassLoader(loader);
      body.run();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      thread.setContextClassLoader(original);
    }
  }

  private File servicesDir(String... providers) throws IOException {
    File root = tmp.newFolder();
    Path services =
        root.toPath().resolve("META-INF/services/org.apache.spark.sql.sources.DataSourceRegister");
    Files.createDirectories(services.getParent());
    Files.write(services, String.join("\n", providers).getBytes(StandardCharsets.UTF_8));
    return root;
  }
}
