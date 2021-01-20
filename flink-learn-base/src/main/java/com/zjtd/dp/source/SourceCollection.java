package com.zjtd.dp.source;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author Wang wenbo
 * @Date 2021/1/20 15:26
 * @Version 1.0
 */
public class SourceCollection {
    public static void main(String[] args) throws Exception {
        // 0.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // 1.Source:读取数据
        DataStreamSource<WaterSensor> sensorDS = env.fromCollection(
                Arrays.asList(
                        new WaterSensor("sensor_1", 15321312412L, 41),
                        new WaterSensor("sensor_2", 15321763412L, 47),
                        new WaterSensor("sensor_3", 15369732412L, 49)
                )
        );

        // 2.打印
        sensorDS.print();

        // 3.执行
        env.execute();
    }
}
