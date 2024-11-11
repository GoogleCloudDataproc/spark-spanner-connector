package com.google.cloud.spark.spanner;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.util.GenericArrayData;

public class SpannerTestUtils {

  public static GenericArrayData zonedDateTimeIterToSparkDates(Iterable<ZonedDateTime> tsIt) {
    List<Integer> dest = new ArrayList<>();
    tsIt.forEach((ts) -> dest.add(SpannerUtils.zonedDateTimeToSparkDate(ts)));
    return new GenericArrayData(dest.toArray(new Integer[0]));
  }

  public static GenericArrayData zonedDateTimeIterToSparkTimestamps(Iterable<ZonedDateTime> tsIt) {
    List<Long> dest = new ArrayList<>();
    tsIt.forEach((ts) -> dest.add(SpannerUtils.zonedDateTimeToSparkTimestamp(ts)));
    return new GenericArrayData(dest.toArray(new Long[0]));
  }
}
