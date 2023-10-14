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

package com.google.cloud.spark.spanner.acceptance;

import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class DataprocImage21AcceptanceTest extends DataprocAcceptanceTestBase {

  private static AcceptanceTestContext context;

  public DataprocImage21AcceptanceTest() {
    super(context);
  }

  @BeforeClass
  public void setup() throws Exception {
    context = this.setup("2.1-debian11", "spark-3.1-spanner", ImmutableList.of());
  }

  @AfterClass
  public void tearDown() throws Exception {
    this.tearDown(context);
  }
}
