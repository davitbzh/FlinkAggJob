package com.logicalclocks.aggregations.functions;

import com.logicalclocks.aggregations.avroSchemas.StoreEvent;
import com.logicalclocks.aggregations.avroSchemas.TestFg;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CountAggregate implements AggregateFunction<StoreEvent, Tuple6<Long, Long, Long, Long, Long, Long>,
    Tuple6<Long, Long, Long, Long, Long, Long>>  {

  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> createAccumulator() {
    return new Tuple6<>(0L,0L,0L,0L,0L,0L);
  }

  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> add(StoreEvent record,
                                                        Tuple6<Long, Long, Long, Long, Long, Long> accumulator) {
    return new Tuple6<>(
        accumulator.f0 + 1,
        Instant.now().toEpochMilli(),
        Math.max(accumulator.f2, record.getReceivedTs()),
        Math.max(accumulator.f3, record.getDeserializationTimestamp()),
        Math.max(accumulator.f4, record.getKafkaCommitTimestamp()),
        0L);
  }

  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> getResult(Tuple6<Long, Long, Long, Long, Long, Long> accumulator) {
    return new Tuple6<>(
        accumulator.f0,
        accumulator.f1,
        accumulator.f2,
        accumulator.f3,
        accumulator.f4,
        accumulator.f5);
  }

  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> merge(Tuple6<Long, Long, Long, Long, Long, Long> accumulator,
                                                          Tuple6<Long, Long, Long, Long, Long, Long> accumulator1) {
    return new Tuple6<>(
        accumulator.f0 + accumulator1.f0,
        accumulator1.f1,
        accumulator1.f2,
        Math.max(accumulator.f3, accumulator1.f3),
        Math.max(accumulator.f4, accumulator1.f4),
        accumulator1.f5);
  }

  public static class MyRichWindowFunction extends RichWindowFunction<Tuple6<Long, Long, Long, Long, Long, Long>,
      byte[], String, TimeWindow> {


    public MyRichWindowFunction(){
    }

    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Tuple6<Long, Long, Long, Long, Long, Long>> iterable,
                      Collector<byte[]> collector) throws Exception {

      Tuple6<Long, Long, Long, Long, Long, Long> agg =  iterable.iterator().next();
      TestFg testFg = new TestFg();

      testFg.setCustomerId(key);
      testFg.setCountAddedToBag(agg.f0);
      testFg.setAggregationStartTime(agg.f1);
      testFg.setMaxEventTimestamp(agg.f2);
      testFg.setMaxDeserializationTimestamp(agg.f3);
      testFg.setMaxKafkaEventCommitTimestamp(agg.f4);

      // Just a dummy values here, online fs will overwrite this values
      testFg.setBenchCommitTime(Instant.now().toEpochMilli());
      testFg.setKafkaTimestamp(Instant.now().toEpochMilli());

      // window end
      testFg.setEnd(timeWindow.getEnd());
      // here it ends
      testFg.setAggregationEndTime(Instant.now().toEpochMilli());
      collector.collect(encode(testFg));
    }

    @Override
    public void open(Configuration parameters) {
    }

    private byte[] encode(TestFg record) throws IOException {

      List<TestFg> records = new ArrayList<>();
      records.add(record);

      DatumWriter<TestFg> datumWriter = new SpecificDatumWriter<TestFg>(record.getSchema());
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      byteArrayOutputStream.reset();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
      for(TestFg segment: records) {
        datumWriter.write(segment, encoder);
      }
      datumWriter.write(record, encoder);
      encoder.flush();
      byte[] bytes = byteArrayOutputStream.toByteArray();
      return bytes;

    }

    private Long processingDelay(long eventTimeStamp) {
      return (Instant.now().toEpochMilli() - eventTimeStamp);
    }

  }
}
