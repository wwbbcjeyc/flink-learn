package com.zjtd.dp.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Wang wenbo
 * @Date 2021/1/20 11:56
 * @Version 1.0
 */
public class TransformShuffle {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env.readTextFile("/Users/wangwenbo/Data/sensor-data.log");
        inputDS.print("input");

        DataStream<String> resultDS = inputDS.shuffle();
        resultDS.print("shuffle");

        env.execute();
    }

}
