package com.logicalclocks.aggregations.functions;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.logicalclocks.aggregations.avroSchemas.*;

import java.time.Instant;

public class MyAvroDeserializer implements KafkaDeserializationSchema<StoreEvent> {

  public MyAvroDeserializer() {
  };

  @Override
  public boolean isEndOfStream(StoreEvent stringObjectMap) {
    return false;
  }

  @Override
  public StoreEvent deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {

    byte[] messageKey = consumerRecord.key();
    byte[] message = consumerRecord.value();
    long offset = consumerRecord.offset();
    long timestamp = consumerRecord.timestamp();


    DeserializationSchema<StoreEvent> deserializer =
        AvroDeserializationSchema.forSpecific(StoreEvent.class);
    StoreEvent deserializedEvent = deserializer.deserialize(message);

    deserializedEvent.setKey(deserializedEvent.getEventDefinitions().getContexts().getUserContext().getCustomerId().toString());
    deserializedEvent.setKafkaCommitTimestamp(timestamp);
    deserializedEvent.setDeserializationTimestamp(Instant.now().toEpochMilli());
    return deserializedEvent;
  }

  @Override
  public TypeInformation<StoreEvent> getProducedType() {
    TypeInformation<StoreEvent> typeInformation = TypeInformation
        .of(new TypeHint<StoreEvent>() {
        });
    return typeInformation;
  }

}
