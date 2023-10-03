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

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class TestData {
  public static List<String> initialDDL = createInitialDDL();
  public static List<String> initialDML = createInitialDML();
  public static List<String> shakespearValues = createShakespeareTableValues();

  private TestData() {}

  private static List<String> readAndParseSQL(String filename) {
    String initialDDL = mustReadResource(filename);
    String[] splits = initialDDL.trim().split(";");
    List<String> stmts = new ArrayList<>();
    for (String stmt : splits) {
      stmt = stmt.trim();
      if (stmt != "" && stmt != "\n") {
        stmts.add(stmt);
      }
    }
    return stmts;
  }

  private static List<String> createInitialDDL() {
    return readAndParseSQL("/db/populate_ddl.sql");
  }

  private static List<String> createInitialDML() {
    return readAndParseSQL("/db/insert_data.sql");
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

  private static List<String> createShakespeareTableValues() {
    String csv = mustReadResource("/db/shakespeare_bq.csv");
    String[] csvLines = csv.trim().split("\n");
    List<String> valueLines = new ArrayList<>();
    Long id = 1L;
    for (String csvLine : csvLines) {
      csvLine = csvLine.trim();
      if (csvLine == "" || csvLine == "\n") {
        continue;
      }

      String[] splits = csvLine.split(",");

      // Now create the lines that'll be inserted per entry.
      // The format is: <id:STRING>, <word:STRING>, <word_count:INT64>, <corpus:STRING>,
      // <corpus_date:INT64>
      valueLines.add(
          String.format(
              "(%d,\"%s\",%s,\"%s\",%s)", id, splits[0], splits[1], splits[2], splits[3]));
      id++;
    }
    return valueLines;
  }
}
