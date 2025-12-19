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

import com.google.cloud.spanner.Mutation;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class TestData {
  static final String WRITE_TABLE_NAME = "write_test_table";
  public static List<String> initialDDL = createInitialDDL("/db/populate_ddl.sql");
  public static List<String> initialDDLPg = createInitialDDL("/db/populate_ddl_pg.sql");
  public static List<String> initialDDLGraph = createInitialDDL("/db/populate_ddl_graph.sql");
  public static List<String> initialDML = createInitialDML("/db/insert_data.sql");
  public static List<String> initialDMLPg = createInitialDML("/db/insert_data_pg.sql");
  public static List<String> initialDMLGraph = createInitialDML("/db/insert_data_graph.sql");
  public static List<Mutation> shakespearMutations = createShakespeareTableMutations();

  private TestData() {}

  private static List<String> readAndParseSQL(String filename) {
    String initialDDL = mustReadResource(filename);
    String[] statements =
        Arrays.stream(initialDDL.split("\\r?\\n"))
            .map(String::trim)
            .filter(l -> !l.startsWith("--"))
            .collect(Collectors.joining("\n"))
            .split(";");
    return Arrays.stream(statements)
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  private static List<String> createInitialDDL(String filePath) {
    return readAndParseSQL(filePath);
  }

  private static List<String> createInitialDML(String filePath) {
    return readAndParseSQL(filePath);
  }

  private static String mustReadResource(String path) {
    try (InputStream stream = TestData.class.getResourceAsStream(path)) {
      String data = CharStreams.toString(new InputStreamReader(Objects.requireNonNull(stream)));
      if (data == null || data.length() == 0) {
        throw new RuntimeException(path + " has no content");
      }
      return data;
    } catch (IOException e) {
      throw new RuntimeException("failed to read resource " + path, e);
    }
  }

  private static List<Mutation> createShakespeareTableMutations() {
    String csv = mustReadResource("/db/shakespeare_bq.csv");
    String[] csvLines = csv.trim().split("\n");
    Long id = 1L;
    List<Mutation> mutations = new ArrayList<>();
    for (String csvLine : csvLines) {
      csvLine = csvLine.trim();
      if (csvLine.equals("") || csvLine.equals("\n")) {
        continue;
      }

      String[] splits = csvLine.split(",");

      mutations.add(
          Mutation.newInsertBuilder("Shakespeare")
              .set("id")
              .to(id)
              .set("word")
              .to(splits[0])
              .set("word_count")
              .to(splits[1])
              .set("corpus")
              .to(splits[2])
              .set("corpus_date")
              .to(splits[3])
              .build());
      id++;
    }
    return mutations;
  }
}
