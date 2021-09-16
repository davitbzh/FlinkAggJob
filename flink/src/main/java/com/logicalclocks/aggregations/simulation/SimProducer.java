package com.logicalclocks.aggregations.simulation;

import com.logicalclocks.aggregations.avroSchemas.StoreEvent;
import com.logicalclocks.aggregations.utils.Utils;
import com.logicalclocks.taxiDemo.ExerciseBase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class SimProducer {
  private Utils utils = new Utils();

  public void run(String topicName, Integer batchSize) throws Exception {
    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(ExerciseBase.parallelism);

    DataStream<StoreEvent> simEvens = env.addSource(new EventSimSourceFunction(batchSize)).keyBy(r -> r.f0)
        .map(new MapFunction<Tuple2<String, StoreEvent>, StoreEvent>() {
      @Override
      public StoreEvent map(Tuple2<String, StoreEvent> storeEvent) throws Exception {
        return storeEvent.f1;
      }
    });

    simEvens
        .rescale()
        .rebalance()
        .addSink(new FlinkKafkaProducer<StoreEvent>(topicName,
        new AvroKafkaSync(topicName),
        utils.getKafkaProperties(),
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));;
    env.execute();
  }

  public static void main(String[] args) throws Exception {

    Options options = new Options();

    options.addOption(Option.builder("topicName")
        .argName("topicName")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("batchSize")
        .argName("batchSize")
        .required(true)
        .hasArg()
        .build());

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    String topicName = commandLine.getOptionValue("topicName");
    Integer batchSize = Integer.parseInt(commandLine.getOptionValue("batchSize"));

    SimProducer simProducer = new SimProducer();
    simProducer.run(topicName, batchSize);
  }
}
