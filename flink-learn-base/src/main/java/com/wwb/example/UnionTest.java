package com.wwb.example;

import com.wwb.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author wangwenbo
 * @Date 2022/5/4 23:11
 * @Version 1.0
 */
public class UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource1 = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> streamSource2 = env.socketTextStream("hadoop103", 8888);

        DataStream<Event> stream1 = streamSource1.map(data -> {
            String[] fields = data.split(" ");
            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        DataStream<Event> stream2 = streamSource2.map(data -> {
            String[] fields = data.split(" ");
            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        //Union合流操作
        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("水位线：" + ctx.timerService().currentWatermark());
                    }
                }).print();

        stream1.print("stream1");
        stream2.print("stream2");

        env.execute();
    }
}

