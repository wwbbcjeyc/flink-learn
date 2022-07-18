package com.wwb.sink;

import com.wwb.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;
/**
 * @Author wangwenbo
 * @Date 2022/5/2 21:56
 * @Version 1.0
 */
public class SinkToKafka {
    public static void main(String[] args) throws Exception{
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 添加kafka配置文件
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");

        //创建kafka Topic
        final String ConsumerTOPIC = "clicks";
        final String ProducerTOPIC = "events";
        // 将字节数据转换成字符串
        SimpleStringSchema schema = new SimpleStringSchema();
        //创建flink 连接  kafka 的对象实例 FlinkKafkaConsumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(ConsumerTOPIC, schema, properties);
        //1,从Kafka中读取数据
        DataStreamSource<String> kafkaStream = env.addSource(kafkaConsumer);

        //2,用Flink进行转换处理
        SingleOutputStreamOperator<String> result = kafkaStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] fields = value.split(" ");
                return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString();
            }
        });

        //3.结果数据写入Kafka
        final String bootstrap = "hadoop102:9092";
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(bootstrap, ProducerTOPIC, schema);

        result.addSink(kafkaProducer);

        env.execute();
    }
}

