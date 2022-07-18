package com.wwb.windows;

import com.wwb.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 22:55
 * @Version 1.0
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //周期性生成watermarkTest
        env.getConfig().setAutoWatermarkInterval(100);

        // 读取数据源，并行度为 1
        DataStream<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "/prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L))
                //乱序的Watermarks
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(2))); //事件时间滚动窗口
//        .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)));// 事件时间滑动窗口
//           .window(EventTimeSessionWindows.withGap(Time.seconds(2)));// 事件时间 会话窗口
//   .countWindow(10,2); // 滑动计数窗口

        env.execute();
    }
}

