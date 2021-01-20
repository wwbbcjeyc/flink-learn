package com.zjtd.dp.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Wang wenbo
 * @Date 2021/1/20 15:28
 * @Version 1.0
 */
public class SourceFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> fileDS = env.readTextFile("/Users/wangwenbo/Data/word.txt");

        fileDS.print();

        env.execute();
    }
}
