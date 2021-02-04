package com.zjtd.dp.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author Wang wenbo
 * @Date 2021/2/4 16:54
 * @Version 1.0
 */
public class UserClickLog2Hdfs {
    public static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserClickLog2Hdfs.class);

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(
                UserClickLog2Hdfs.class.getClassLoader().getResourceAsStream("config.properties"));

        String bootstrapServers = parameterTool.getRequired("bootstrap.servers");
        String groupId = parameterTool.getRequired("group.id");
        String autoOffsetReset = parameterTool.getRequired("auto.offset.reset");
        String kafka2HdfsTopics = parameterTool.getRequired("kafka2hdfs.topics");
        long kafkaConsumeFrom = parameterTool.getLong("kafka2hdfs.kafka.consume.from", 0);
        int sourceRawDataParallelism = parameterTool.getInt("source.raw.data.parallelism");
        String checkpointDataUri = parameterTool.getRequired("checkpoint.data.uri");
        String kafka2HdfsUri = parameterTool.getRequired("kafka2hdfs.uri");
        long rolloverInterval = parameterTool.getLong("kafka2hdfs.rollover.interval");
        long inactivityInterval = parameterTool.getLong("kafka2hdfs.inactivity.interval");
        // 单位是M
        long maxPartSize = parameterTool.getLong("kafka2hdfs.max.part.size");
        System.out.println(rolloverInterval);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // Checkpoint
       /* env.enableCheckpointing(300000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);*/
        // StateBackend
        //env.setStateBackend((StateBackend) new RocksDBStateBackend(checkpointDataUri, true));
        // GlobalJobParameters
        //env.getConfig().setGlobalJobParameters(parameterTool);
        // RestartStrategy
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(1, TimeUnit.MINUTES)));

        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".txt")
                .build();

        String outputPath="E:\\data\\out";


        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new EventTimeBucketAssigner<>())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.SECONDS.toMillis(2)) //设置滚动间隔
                        .withInactivityInterval(TimeUnit.SECONDS.toMillis(1)) //设置不活动时间间隔
                        .withMaxPartSize(1024 * 1024 * 1024) // 最大零件尺寸
                        .build())
                .withOutputFileConfig(config)
                .build();

        List<String> topics = Arrays.asList(kafka2HdfsTopics.split(","));
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), consumerProps);
        // # EARLIEST=0L, LATEST=1L, TIMESTAMP=timestampL
        if (kafkaConsumeFrom == 0) {
            logger.warn("flink consume kafka from earliest......");
            kafkaConsumer.setStartFromEarliest();
        } else if (kafkaConsumeFrom == 1) {
            logger.warn("flink consume kafka from latest");
            kafkaConsumer.setStartFromLatest();
        } else {
            logger.warn("flink consume kafka from timestamp: " + kafkaConsumeFrom);
            kafkaConsumer.setStartFromTimestamp(kafkaConsumeFrom);
        }
        env.addSource(kafkaConsumer).setParallelism(sourceRawDataParallelism)
                .name("kf2hdfs").uid("kf2hdfs")
                .addSink(sink);

        env.execute(UserClickLog2Hdfs.class.getCanonicalName());


    }
}
