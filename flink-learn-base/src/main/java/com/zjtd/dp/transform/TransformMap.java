package com.zjtd.dp.transform;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Wang wenbo
 * @Date 2021/1/20 10:57
 * @Version 1.0
 */
public class TransformMap {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env.readTextFile("/Users/wangwenbo/Data/sensor-data.log");

        // 2.Transform: Map转换成实体对象
        SingleOutputStreamOperator<WaterSensor> sensorDS = inputDS.map(new MyMapFunction());

        // 3.打印
        sensorDS.print();

        env.execute();
    }

    /**
     * 实现MapFunction，指定输入的类型，返回的类型
     * 重写 map方法
     */
    public static class MyMapFunction implements MapFunction<String, WaterSensor> {

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] datas = value.split(",");
            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        }
    }
}
