package com.google.cloud.spark.spanner.integration;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spark.spanner.SpannerConnectorException;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class Spark31QueryReadIntegrationTest extends SparkSpannerIntegrationTestBase {

  @Test
  public void readsPartitionableQueryWithWildcardProjection() {
    Dataset<Row> dataframe = readQuery("SELECT * FROM ATable");

    assertThat(dataframe.columns())
        .asList()
        .containsExactly("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L")
        .inOrder();
    assertThat(dataframe.count()).isEqualTo(5);
  }

  @Test
  public void readsPartitionableQueryWithAliasesAndFilters() {
    Dataset<Row> dataframe = readQuery("SELECT A AS amount, B FROM ATable WHERE A >= 10");

    assertThat(dataframe.columns()).asList().containsExactly("amount", "B").inOrder();
    assertThat(dataframe.rdd().getNumPartitions()).isGreaterThan(0);
    assertThat(dataframe.count()).isEqualTo(4);
    assertThat(dataframe.filter("amount = 10").first().getLong(0)).isEqualTo(10);
  }

  @Test
  public void infersArrayAndJsonResultSchemas() {
    Dataset<Row> dataframe = readQuery("SELECT id, A, B, K FROM compositeTable WHERE id = 'id1'");

    assertThat(dataframe.schema().apply("A").dataType())
        .isEqualTo(DataTypes.createArrayType(DataTypes.LongType, true));
    assertThat(dataframe.schema().apply("B").dataType())
        .isEqualTo(DataTypes.createArrayType(DataTypes.StringType, true));
    assertThat(dataframe.first().getList(1)).containsExactly(10L, 100L, 991L, 567282L).inOrder();
    assertThat(dataframe.first().getString(3)).contains("\"a\":1");
  }

  @Test
  public void queryCannotBeCombinedWithTable() {
    Map<String, String> properties = connectionProperties();

    assertThrows(
        SpannerConnectorException.class,
        () ->
            spark
                .read()
                .format("cloud-spanner")
                .options(properties)
                .option("query", "SELECT A FROM ATable")
                .load());
  }

  @Test
  public void rejectsMutatingSql() {
    assertThrows(SpannerConnectorException.class, () -> readQuery("DELETE FROM ATable WHERE true"));
  }

  private Dataset<Row> readQuery(String query) {
    Map<String, String> properties = new HashMap<>(connectionProperties());
    properties.remove("table");
    return spark.read().format("cloud-spanner").options(properties).option("query", query).load();
  }
}
