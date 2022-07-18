package com.wwb.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 21:51
 * @Version 1.0
 */
public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 添加kafka配置文件
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.100.141.141:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //创建kafka Topic
        final String TOPIC = "tuhutest_default_cl.data.track.topic";
        // 将字节数据转换成字符串
        SimpleStringSchema schema = new SimpleStringSchema();
        //创建flink 连接  kafka 的对象实例 FlinkKafkaConsumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC, schema, properties);
        // 添加数据源
        DataStreamSource<String> stream = env.addSource(kafkaConsumer);

        stream.print("Kafka");

        env.execute();
    }
}
