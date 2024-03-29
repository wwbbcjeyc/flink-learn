package com.wwb.windows;

import com.wwb.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * @Author wangwenbo
 * @Date 2022/5/2 22:59
 * @Version 1.0
 */
public class TimeCharacteristicWindowAssigner {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        // 1.
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                })
                // TODO 2.指定如何 从数据中 抽取出 事件时间，时间单位是 ms
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }));


        // 分组、开窗、聚合
        // TODO 窗口是如何分配的？ 开始时间、结束时间
        // 窗口的开始时间 => timestamp - (timestamp + windowSize) % windowSize;
        //              => 1549044122 - (1549044122 + 5) % 5  = 1549044120 => 向下取整（整：窗口长度的整数倍）
        //              => 1549044127 - (1549044127 + 5) % 5  = 1549044125
        // 窗口的结束时间 => start + size => 窗口的开始时间 + 窗口长度
        // 窗口是 左闭右开  =>  maxTimestamp = end - 1; =》

        // TODO 窗口是如何触发计算的？
        // window.maxTimestamp() <= ctx.getCurrentWatermark()
        // end - 1 <= watermark


        sensorDS
                .keyBy(data -> data.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(
                        /**
                         * 全窗口函数：整个窗口的本组数据，存起来，关窗的时候一次性一起计算
                         */
                        new ProcessWindowFunction<WaterSensor, Long, String, TimeWindow>() {

                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<Long> out) throws Exception {
                                out.collect(elements.spliterator().estimateSize());

                            }
                        })
                .print();


        env.execute();
    }
}

