package com.google.cloud.spark.spanner;

import static com.google.common.truth.Truth.assertThat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.openlineage.spark.agent.OpenLineageSparkListener;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

public class OpenLineageIntegrationTestBase extends SpannerTestBase {

  @ClassRule public static OLSparkFactory sparkFactory = new OLSparkFactory();

  protected SparkSession spark;
  protected File lineageOutputFile;

  protected Map<String, String> connectionProperties;

  public OpenLineageIntegrationTestBase() {
    this.spark = sparkFactory.spark;
    this.lineageOutputFile = sparkFactory.lineageOutputFile;
    this.connectionProperties = connectionProperties();
  }

  protected static class OLSparkFactory extends ExternalResource {
    SparkSession spark;

    File lineageOutputFile;

    @Override
    protected void before() throws Throwable {
      lineageOutputFile = File.createTempFile("openlineage_test_" + System.nanoTime(), ".log");
      lineageOutputFile.deleteOnExit();
      spark =
          SparkSession.builder()
              .master("local")
              .config("spark.ui.enabled", "false")
              .config("spark.default.parallelism", 20)
              .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
              .config("spark.openlineage.transport.type", "file")
              .config("spark.openlineage.transport.location", lineageOutputFile.getAbsolutePath())
              .getOrCreate();
      spark.sparkContext().setLogLevel("WARN");
    }
  }

  public Dataset<Row> readFromTable(String table) {
    Map<String, String> props = this.connectionProperties();
    return spark
        .read()
        .format("cloud-spanner")
        .option("viewsEnabled", true)
        .option("projectId", props.get("projectId"))
        .option("instanceId", props.get("instanceId"))
        .option("databaseId", props.get("databaseId"))
        .option("emulatorHost", props.get("emulatorHost"))
        .option("table", table)
        .load();
  }

  @Test
  public void testOpenLineageEvents() throws Exception {
    File outputCsv = File.createTempFile("output_" + System.nanoTime(), ".csv");
    outputCsv.deleteOnExit();
    Dataset<Row> df = readFromTable("compositeTable");
    df.createOrReplaceTempView("tempview");
    Dataset<Row> outputDf =
        spark.sql(
            "SELECT word, count(*) AS count FROM (SELECT explode(split(C, ' ')) AS word FROM tempview) GROUP BY 1");

    outputDf
        .coalesce(1)
        .write()
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save("file://" + outputCsv.getPath());

    List<JsonObject> jsonObjects = parseEventLog(lineageOutputFile);
    assertThat(jsonObjects).isNotEmpty();

    jsonObjects.forEach(
        jsonObject -> {
          JsonObject input = jsonObject.getAsJsonArray("inputs").get(0).getAsJsonObject();
          assertThat(input.get("namespace").getAsString())
              .isEqualTo(
                  String.format(
                      "spanner://%s/%s",
                      connectionProperties.get("projectId"),
                      connectionProperties.get("instanceId")));
          assertThat(input.get("name").getAsString())
              .isEqualTo(
                  String.format("%s/%s", connectionProperties.get("databaseId"), "compositeTable"));
        });
  }

  private List<JsonObject> parseEventLog(File file) throws Exception {
    List<JsonObject> eventList;
    try (Scanner scanner = new Scanner(file)) {
      eventList = new ArrayList<>();
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        JsonObject event = JsonParser.parseString(line).getAsJsonObject();
        if (!event.getAsJsonArray("inputs").isEmpty()) {
          eventList.add(event);
        }
      }
    }
    return eventList;
  }
}
