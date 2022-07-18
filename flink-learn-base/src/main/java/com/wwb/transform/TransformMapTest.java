package com.wwb.transform;

import com.wwb.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 22:13
 * @Version 1.0
 */
public class TransformMapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 进行转换计算,实现user 字段
        // 1,使用自定义类，实现MapFunction接口
        stream.map(new UserExtractor()).print("1");

        // 2,传入匿名类，实现MapFunction
        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        }).print("2");

        //3,使用Lambda表达式
        stream.map( data -> data.user).print("3");

        env.execute();
    }

    //自定义 MapFunction
    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}


