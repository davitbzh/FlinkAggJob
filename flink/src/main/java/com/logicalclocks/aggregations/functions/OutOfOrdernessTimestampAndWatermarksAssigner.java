package com.logicalclocks.aggregations.functions;

import com.logicalclocks.aggregations.avroSchemas.StoreEvent;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

public class OutOfOrdernessTimestampAndWatermarksAssigner
    extends BoundedOutOfOrdernessTimestampExtractor<StoreEvent>  {

  private String timestampField;

  public OutOfOrdernessTimestampAndWatermarksAssigner(Time maxOutOfOrderness, String timestampField) {
    super(maxOutOfOrderness);
    this.timestampField = timestampField;
  }

  @SneakyThrows
  @Override
  public long extractTimestamp(StoreEvent element) {
    return element.getReceivedTs();
  }
}
