package com.wwb.transform;

import com.wwb.bean.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 22:13
 * @Version 1.0
 */
public class TransformFlatmapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        //1,实现FlatMapFunction
        stream.flatMap(new MyFlatMap()).print("1");

        //2,传入Lambda表达式
        SingleOutputStreamOperator<String> returns = stream.flatMap((Event value, Collector<String> out) -> {
            if (value.user.equals("Bob")) {
                out.collect(value.user);
            } else if (value.user.equals("Alice")) {
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {
        });

        env.execute();
    }

    // 实现一个自定义的 FlatMapFunction 函数
    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }
    }
}

