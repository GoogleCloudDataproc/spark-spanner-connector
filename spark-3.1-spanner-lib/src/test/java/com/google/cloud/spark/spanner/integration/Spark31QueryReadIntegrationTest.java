package com.google.cloud.spark.spanner.integration;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spark.spanner.SpannerConnectorException;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class Spark31QueryReadIntegrationTest extends SparkSpannerIntegrationTestBase {

  @Test
  public void readsPartitionableQueryWithWildcardProjection() {
    Dataset<Row> dataframe = readQuery("SELECT * FROM ATable");

    assertThat(dataframe.columns())
        .asList()
        .containsExactly("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M")
        .inOrder();
    assertThat(dataframe.count()).isEqualTo(5);
  }

  @Test
  public void readsFloat32QueryResults() {
    Dataset<Row> dataframe =
        readQuery(
            "SELECT A, M, CAST('NaN' AS FLOAT32) AS f32_nan,"
                + " CAST('Infinity' AS FLOAT32) AS f32_pos_inf,"
                + " CAST('-Infinity' AS FLOAT32) AS f32_neg_inf,"
                + " [M, CAST(NULL AS FLOAT32), CAST('NaN' AS FLOAT32)] AS f32_array"
                + " FROM ATable WHERE A IN (1, 40, 50)");

    StructType schema = dataframe.schema();
    assertThat(schema.apply("M").dataType()).isEqualTo(DataTypes.FloatType);
    assertThat(schema.apply("f32_nan").dataType()).isEqualTo(DataTypes.FloatType);
    assertThat(schema.apply("f32_array").dataType())
        .isEqualTo(DataTypes.createArrayType(DataTypes.FloatType, true));

    Map<Long, Row> rows = new HashMap<>();
    dataframe.collectAsList().forEach(row -> rows.put(row.getLong(0), row));
    assertThat(rows.keySet()).containsExactly(1L, 40L, 50L);

    Row row = rows.get(1L);
    assertThat(row.getFloat(1)).isEqualTo(-0.1f);
    assertThat(row.getFloat(2)).isNaN();
    assertThat(row.getFloat(3)).isPositiveInfinity();
    assertThat(row.getFloat(4)).isNegativeInfinity();
    assertThat(row.getList(5)).containsExactly(-0.1f, null, Float.NaN).inOrder();

    assertThat(rows.get(40L).getFloat(1)).isEqualTo(1.25f);
    assertThat(rows.get(40L).getList(5)).containsExactly(1.25f, null, Float.NaN).inOrder();

    assertThat(rows.get(50L).isNullAt(1)).isTrue();
    assertThat(rows.get(50L).getList(5)).containsExactly(null, null, Float.NaN).inOrder();
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
