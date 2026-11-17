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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.sources.DataSourceRegister;

/**
 * Decides which Spanner provider claims the {@code cloud-spanner} short name.
 *
 * <p>Each Spark-version-specific artifact registers its own provider class ({@code
 * Spark35SpannerTableProvider}, {@code Spark41SpannerTableProvider}, {@code
 * Spark42SpannerTableProvider}, ...), and the artifact for a newer Spark version also contains the
 * provider classes of all older versions. When two artifacts are on the classpath, for example one
 * bundled with the Spark distribution and one passed with {@code --jars}, Spark's {@code
 * DataSource.lookupDataSource} finds two distinct classes for {@code cloud-spanner} and fails with
 * "Multiple sources found for cloud-spanner".
 *
 * <p>To avoid that, a provider gives up the short name when it does not target the running Spark
 * version and another registered Spanner provider does. In that case it answers to {@code
 * cloud-spanner-<major.minor>} instead. In every other case the provider keeps {@code
 * cloud-spanner}, so a single artifact keeps working on any Spark version it worked on before.
 *
 * <p>This also covers jars built before this change: with the default parent-first class loading,
 * the class named in an older jar's services file resolves to the copy in the bundled (newer) jar,
 * which runs this logic.
 */
final class SpannerShortName {

  static final String SHORT_NAME = "cloud-spanner";

  private static final String SERVICES_FILE =
      "META-INF/services/" + DataSourceRegister.class.getName();

  private static final Pattern PROVIDER_CLASS =
      Pattern.compile(
          "com\\.google\\.cloud\\.spark\\.spanner\\.Spark(\\d)(\\d+)SpannerTableProvider");

  private static final Pattern MAJOR_MINOR = Pattern.compile("^(\\d+)\\.(\\d+)");

  private SpannerShortName() {}

  /** Returns the short name that an instance of {@code providerClass} should report. */
  static String forProvider(Class<?> providerClass) {
    String target = targetSparkVersion(providerClass);
    String running = majorMinor(org.apache.spark.package$.MODULE$.SPARK_VERSION());
    if (target == null || running == null || target.equals(running)) {
      // Matching providers skip the services scan, which keeps the common case cheap.
      return SHORT_NAME;
    }
    return choose(target, running, registeredProviderVersions(classLoader(providerClass)));
  }

  /**
   * Returns {@code cloud-spanner} unless a registered provider targets {@code running} exactly and
   * this provider (targeting {@code target}) does not.
   */
  static String choose(String target, String running, Collection<String> registeredTargets) {
    if (target == null
        || running == null
        || target.equals(running)
        || !registeredTargets.contains(running)) {
      return SHORT_NAME;
    }
    return SHORT_NAME + "-" + target;
  }

  /**
   * Returns the Spark version targeted by the first versioned provider class in the hierarchy of
   * {@code cls} (so {@code DefaultSource} maps to its parent provider), or null if there is none.
   */
  static String targetSparkVersion(Class<?> cls) {
    for (Class<?> c = cls; c != null; c = c.getSuperclass()) {
      String version = targetSparkVersion(c.getName());
      if (version != null) {
        return version;
      }
    }
    return null;
  }

  /** Returns "4.1" for "com.google.cloud.spark.spanner.Spark41SpannerTableProvider", else null. */
  static String targetSparkVersion(String className) {
    Matcher m = PROVIDER_CLASS.matcher(className);
    return m.matches() ? m.group(1) + "." + m.group(2) : null;
  }

  /** Returns "4.2" for "4.2.0" or "4.2.0-SNAPSHOT", else null. */
  static String majorMinor(String sparkVersion) {
    if (sparkVersion == null) {
      return null;
    }
    Matcher m = MAJOR_MINOR.matcher(sparkVersion);
    return m.find() ? m.group(1) + "." + m.group(2) : null;
  }

  /**
   * Returns the Spark versions targeted by the Spanner providers listed in the {@code
   * DataSourceRegister} services files visible to {@code loader}. Only the file contents are read,
   * so no provider gets instantiated (which would recurse into {@code shortName()}).
   */
  static Set<String> registeredProviderVersions(ClassLoader loader) {
    Set<String> versions = new HashSet<>();
    if (loader == null) {
      return versions;
    }
    try {
      Enumeration<URL> urls = loader.getResources(SERVICES_FILE);
      while (urls.hasMoreElements()) {
        addProviderVersions(urls.nextElement(), versions);
      }
    } catch (IOException e) {
      // Fall through: with nothing found, the provider keeps the short name as before this change.
    }
    return versions;
  }

  private static void addProviderVersions(URL url, Set<String> versions) throws IOException {
    try (InputStream in = url.openStream();
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        int comment = line.indexOf('#');
        String name = (comment >= 0 ? line.substring(0, comment) : line).trim();
        String version = targetSparkVersion(name);
        if (version != null) {
          versions.add(version);
        }
      }
    }
  }

  // Mirrors Spark's Utils.getContextOrSparkClassLoader, which DataSource.lookupDataSource uses.
  private static ClassLoader classLoader(Class<?> providerClass) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    return loader != null ? loader : providerClass.getClassLoader();
  }
}
