package com.wwb.state;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:50
 * @Version 1.0
 */
public class CheckPointTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //每隔1秒中就触发一次检查点操作
        env.enableCheckpointing(1000L);

        //HashMapStateBackend 状态后端
        env.setStateBackend(new HashMapStateBackend());

        //EmbeddedRocksDBStateBackend 状态后端
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.execute();
    }
}
