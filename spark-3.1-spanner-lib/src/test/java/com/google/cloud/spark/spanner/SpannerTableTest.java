package com.google.cloud.spark;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spark.spanner.SpannerScanBuilder;
import com.google.cloud.spark.spanner.SpannerTable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerTableTest {

  @Before
  public void setUp() throws Exception {
    SpannerUtilsTest ss = new SpannerUtilsTest();
    ss.initDatabase();
  }

  @Test
  public void createSchema() {
    Map<String, String> props = SpannerUtilsTest.connectionProperties();
    SpannerTable st = new SpannerTable(null, props);
    StructType actualSchema = st.schema();
    StructType expectSchema =
        new StructType(
            Arrays.asList(
                    new StructField("A", DataTypes.LongType, false, null),
                    new StructField("B", DataTypes.StringType, true, null),
                    new StructField(
                        "C", DataTypes.createArrayType(DataTypes.ByteType, true), true, null),
                    new StructField("D", DataTypes.TimestampType, true, null),
                    new StructField("E", DataTypes.createDecimalType(38, 9), true, null),
                    new StructField(
                        "F", DataTypes.createArrayType(DataTypes.StringType, true), true, null))
                .toArray(new StructField[0]));

    // Object.equals fails for StructType with fields so we'll
    // firstly compare lengths, then fieldNames then the simpleString.
    assertEquals(expectSchema.length(), actualSchema.length());
    assertEquals(expectSchema.fieldNames(), actualSchema.fieldNames());
    assertEquals(expectSchema.simpleString(), actualSchema.simpleString());
  }

  @Test
  public void show() {
    Map<String, String> props = SpannerUtilsTest.connectionProperties();
    SpannerTable st = new SpannerTable(null, props);
    CaseInsensitiveStringMap csm = new CaseInsensitiveStringMap(props);
    ScanBuilder sb = st.newScanBuilder(csm);
    SpannerScanBuilder ssb = ((SpannerScanBuilder) sb);
    InputPartition[] parts = ssb.planInputPartitions();
    PartitionReaderFactory prf = ssb.createReaderFactory();

    for (InputPartition part : parts) {
      PartitionReader<InternalRow> ir = prf.createReader(part);
      try {
        while (ir.next()) {
          InternalRow row = ir.get();
          System.out.println("row: " + row.toString());
        }
      } catch (IOException e) {
      }
    }
  }
}
