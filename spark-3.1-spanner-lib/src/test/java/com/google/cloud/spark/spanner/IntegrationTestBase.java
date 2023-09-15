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

package com.google.cloud.spark;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.grpc.auth.MoreCallCredentials;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IntegrationTestBase {

  @ClassRule public static SparkFactory sparkFactory = new SparkFactory();

  protected SparkSession spark;

  private static final String STATIC_OAUTH_TOKEN = "STATIC_TEST_OAUTH2_TOKEN";
  private static final String VARIABLE_OAUTH_TOKEN = "VARIABLE_TEST_OAUTH2_TOKEN";
  private static final OAuth2Credentials STATIC_CREDENTIALS =
      OAuth2Credentials.create(
          new AccessToken(
              STATIC_OAUTH_TOKEN,
              new java.util.Date(
                  System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(1L, TimeUnit.DAYS))));
  private static final OAuth2Credentials VARIABLE_CREDENTIALS =
      OAuth2Credentials.create(
          new AccessToken(
              VARIABLE_OAUTH_TOKEN,
              new java.util.Date(
                  System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(1L, TimeUnit.DAYS))));

  public IntegrationTestBase() {
    this.spark = sparkFactory.spark;
  }

  @Test
  public void testWholeSetup() {}

  @Before
  public void createTests() {}

  // Point to the cloud spanner emulator if running local.
  private static Spanner createSpanner(String endpoint, String projectId) {
    return SpannerOptions.newBuilder()
        .setProjectId(projectId)
        // Sets a custoom channel configurator to enable HTTP not HTTPS.
        .setChannelConfigurator(
            input -> {
              input.usePlaintext();
              return input;
            })
        .setHost("http://" + endpoint)
        .setCredentials(STATIC_CREDENTIALS)
        .setCallCredentialsProvider(() -> MoreCallCredentials.from(VARIABLE_CREDENTIALS))
        .build()
        .getService();
  }

  // SparkFactory will have an interceptor that links "cloud-spanner"
  // to our DefaultTableProvider.
  protected static class SparkFactory extends ExternalResource {
    SparkSession spark;

    @Override
    protected void before() throws Throwable {
      spark =
          SparkSession.builder()
              .master("local")
              .config("spark.ui.enabled", "false")
              .config("spark.default.parallelism", 20)
              .getOrCreate();
      // Reduce the test logs spamming.
      spark.sparkContext().setLogLevel("WARN");
    }
  }
}
