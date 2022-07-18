package com.wwb.example;

import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
/**
 * @Author wangwenbo
 * @Date 2022/5/4 23:10
 * @Version 1.0
 */
public class SplitStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 定义输出标签，侧输出流的数据类型为三元组(user, url, timestamp)
        OutputTag<Tuple3<String, String, Long>> MaryTag = new OutputTag<Tuple3<String, String, Long>>("Mary") {
        };
        OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob") {
        };
        //准备数据源
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());
        // 准备分流操作
        SingleOutputStreamOperator<Event> processStream = stream.process(new ProcessFunction<Event, Event>() {

            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                //如果数据user 是Mary 则单独打印
                if (value.user.equals("Mary")) {
                    ctx.output(MaryTag, Tuple3.of(value.user, value.url, value.timestamp));
                    //如果数据user 是Bob 则单独打印
                } else if (value.user.equals("Bob")) {
                    ctx.output(BobTag, Tuple3.of(value.user, value.url, value.timestamp));
                    //如果数据user 是其他数据,则打印
                } else {
                    out.collect(new Event(value.user, value.url, value.timestamp));
                }
            }
        });

        processStream.print("Event");
        processStream.getSideOutput(MaryTag).print("Mary");
        processStream.getSideOutput(BobTag).print("Bob");

        env.execute();
    }
}

