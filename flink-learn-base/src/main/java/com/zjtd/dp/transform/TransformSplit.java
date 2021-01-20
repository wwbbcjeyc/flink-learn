/*
package com.zjtd.dp.transform;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

*/
/**
 * @Author Wang wenbo
 * @Date 2021/1/20 11:56
 * @Version 1.0
 *//*

public class TransformSplit {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("/Users/wangwenbo/Data/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });

        // TODO Split: 水位低于 50 正常，水位 [50，80) 警告， 水位高于 80 告警
        // split并不是真正的把流分开
        SplitStream<WaterSensor> splitSS = sensorDS.split(new OutputSelector<WaterSensor>() {
                                                              @Override
                                                              public Iterable<String> select(WaterSensor value) {
                                                                  if (value.getVc() < 50) {
                                                                      return Arrays.asList("normal");
                                                                  } else if (value.getVc() < 80) {
                                                                      return Arrays.asList("warn");
                                                                  } else {
                                                                      return Arrays.asList("alarm");
                                                                  }
                                                              }
                                                          }
        );

        env.execute();
    }
}
*/
