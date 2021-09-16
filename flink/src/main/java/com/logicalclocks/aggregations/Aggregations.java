package com.logicalclocks.aggregations;

import com.logicalclocks.aggregations.avroSchemas.StoreEvent;
import com.logicalclocks.aggregations.functions.CountAggregate;
import com.logicalclocks.aggregations.functions.OnEveryElementTrigger;
import com.logicalclocks.aggregations.simulation.EventSimSourceFunction;
import com.logicalclocks.aggregations.synk.AvroKafkaSink;
import com.logicalclocks.aggregations.utils.Utils;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class Aggregations {

  private Utils utils = new Utils();

  public void run(String projectName,
                  String featureGroupName,
                  Integer featureGroupVersion,
                  String stateBackend,
                  String keyName,
                  String timestampField,
                  String sourceTopic,
                  Integer windowSize,
                  String windowTimeUnit,
                  Integer watermark,
                  String watermarkTimeUnit,
                  String windowType,
                  Integer slideSize,
                  String slideTimeUnit,
                  Integer gapSize,
                  String gapTimeUnit,
                  Integer parallelism,
                  Boolean withSimulation,
                  Integer batchSize
  ) throws Exception {

    // define stream env
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(parallelism);
    env.enableCheckpointing(30000);

    if (stateBackend.equals("rocksDBStateBackend")) {
      RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs:///Projects/" +
          projectName + "/Resources/flink", true);
      env.setStateBackend(rocksDBStateBackend);
    } else if (stateBackend.equals("fsStateBackend")) {
      FsStateBackend fsStateBackend = new FsStateBackend("hdfs:///Projects/" +
          projectName + "/Resources/flink");
      env.setStateBackend(fsStateBackend);
    }

    // get source stream
    DataStream<StoreEvent> sourceStream;

    if (withSimulation) {
      sourceStream = env.addSource(new EventSimSourceFunction(batchSize)).keyBy(r -> r.f0)
          .map(new MapFunction<Tuple2<String, StoreEvent>, StoreEvent>() {
            @Override
            public StoreEvent map(Tuple2<String, StoreEvent> storeEvent) throws Exception {
              return storeEvent.f1;
            }
          });
    } else {
      sourceStream = utils.getSourceKafkaStream(env, sourceTopic,
          timestampField, watermark, watermarkTimeUnit);
    }

    // get hsfs handle
    FeatureStore fs = utils.getFeatureStoreHandle();

    // get feature groups
    FeatureGroup featureGroup = fs.getFeatureGroup(featureGroupName, featureGroupVersion);

    DataStream<byte[]> aggregationStream = sourceStream
        .rescale()
        .rebalance()
        .keyBy(r -> r.getKey().toString())
        .filter(r -> r.getEventType().toString().equals("ADD_TO_BAG"))
        .keyBy(r -> r.getKey().toString())
        .window(utils.inferWindowType(windowType, utils.inferTimeSize(windowSize, windowTimeUnit),
            utils.inferTimeSize(slideSize, slideTimeUnit), utils.inferTimeSize(gapSize, gapTimeUnit)))
        .trigger(new OnEveryElementTrigger())
        .aggregate(new CountAggregate(), new CountAggregate.MyRichWindowFunction());

    //send to online fg topic
    Properties featureGroupKafkaPropertiies = utils.getKafkaProperties(featureGroup);
    aggregationStream
        .rescale()
        .rebalance()
        .addSink(new FlinkKafkaProducer<byte[]>(featureGroup.getOnlineTopicName(),
        new AvroKafkaSink(keyName, featureGroup.getOnlineTopicName()),
        featureGroupKafkaPropertiies,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    env.execute("Window aggregation of " + windowType);
  }

  public static void main(String[] args) throws Exception {

    Options options = new Options();

    options.addOption(Option.builder("projectName")
        .argName("projectName")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("featureGroupName")
        .argName("featureGroupName")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("featureGroupVersion")
        .argName("featureGroupVersion")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("stateBackend")
        .argName("stateBackend")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("keyName")
        .argName("keyName")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("timestampField")
        .argName("timestampField")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("sourceTopic")
        .argName("sourceTopic")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("windowSize")
        .argName("windowSize")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("windowTimeUnit")
        .argName("windowTimeUnit")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("watermark")
        .argName("watermark")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("watermarkTimeUnit")
        .argName("watermarkTimeUnit")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("windowType")
        .argName("windowType")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("slideSize")
        .argName("slideSize")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("slideSize")
        .argName("slideSize")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("slideTimeUnit")
        .argName("slideTimeUnit")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("gapSize")
        .argName("gapSize")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("gapTimeUnit")
        .argName("gapTimeUnit")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("parallelism")
        .argName("parallelism")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("withSimulation")
        .argName("withSimulation")
        .required(false)
        .hasArg()
        .build());

    options.addOption(Option.builder("batchSize")
        .argName("batchSize")
        .required(false)
        .hasArg()
        .build());

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    String projectName = commandLine.getOptionValue("projectName");
    String featureGroupName = commandLine.getOptionValue("featureGroupName");
    Integer featureGroupVersion = Integer.parseInt(commandLine.getOptionValue("featureGroupVersion"));
    String stateBackend = commandLine.getOptionValue("stateBackend");
    String keyName = commandLine.getOptionValue("keyName");
    String timestampField = commandLine.getOptionValue("timestampField");
    String sourceTopic = commandLine.getOptionValue("sourceTopic");
    Integer windowSize = Integer.parseInt(commandLine.getOptionValue("windowSize"));
    String windowTimeUnit = commandLine.getOptionValue("windowTimeUnit");
    Integer watermark = Integer.parseInt(commandLine.getOptionValue("watermark"));
    String watermarkTimeUnit = commandLine.getOptionValue("watermarkTimeUnit");
    String windowType = commandLine.getOptionValue("windowType");
    Integer slideSize = Integer.parseInt(commandLine.getOptionValue("slideSize"));
    String slideTimeUnit = commandLine.getOptionValue("slideTimeUnit");
    Integer gapSize = Integer.parseInt(commandLine.getOptionValue("gapSize"));
    String gapTimeUnit = commandLine.getOptionValue("gapTimeUnit");
    Integer parallelism = Integer.parseInt(commandLine.getOptionValue("parallelism"));

    Boolean withSimulation = Boolean.parseBoolean(commandLine.getOptionValue("withSimulation"));
    Integer batchSize = Integer.parseInt(commandLine.getOptionValue("batchSize"));

    Aggregations aggregations = new Aggregations();
    aggregations.run(projectName,
                     featureGroupName,
                     featureGroupVersion,
                     stateBackend,
                     keyName,
                     timestampField,
                     sourceTopic,
                     windowSize,
                     windowTimeUnit,
                     watermark,
                     watermarkTimeUnit,
                     windowType,
                     slideSize,
                     slideTimeUnit,
                     gapSize,
                     gapTimeUnit,
                     parallelism,
                     withSimulation,
                     batchSize);
  }
}
