package com.wwb.process;

import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:27
 * @Version 1.0
 */
public class ProcessFunctionTest01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据，并提取时间戳、生成水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.process(new ProcessFunction<Event, String>() {

            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                //如果user是 Mary ，则输出 user - clicks - url
                if (value.user.equals("Mary")) {
                    out.collect(value.user + " - clicks - " + value.url);
                    //如果user是Bob 则输出两次 Bob
                } else if (value.user.equals("Bob")) {
                    out.collect(value.user);
                    out.collect(value.user);
                }

                //输出所有的event类型
                out.collect(value.toString());
                System.out.println("time:" + ctx.timestamp()); // 获取数据当前的时间
                System.out.println("watermark:" + ctx.timerService().currentWatermark()); // 获取数据当前的watermark


                System.out.println(getRuntimeContext().getIndexOfThisSubtask());

            }
        }).print();

        env.execute();

    }

}
