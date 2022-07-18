package com.wwb.example;


import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:31
 * @Version 1.0
 */
public class TopNExample_ProcessAllWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        //直接开窗，直接收集所有数据
        /**
         * 获取当前数据的url,根据滑动窗口，开一个10s的窗口，每隔5s滑动一次
         * 启动增量聚合函数和全窗口函数
         */
        stream.map(data -> data.url)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapAgg(), new UrlAllWindowResult())
                .print();

        env.execute();

    }

    /**
     * String : 定义的输入类型，对标user类型
     * HashMap<String,Long> : 每来一个user类型，就做 +1 操作
     * ArrayList<Tuple2<String,Long>> :输出类型，HashMap的返回操作
     */

    //实现自定义的增量聚合函数
    public static class UrlHashMapAgg implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>> {

        @Override
        public HashMap<String, Long> createAccumulator() {
            //定义初始化 HashMap
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
            /*
             * 查看当前url是否存在，如果存在，则获取value，如果不存在
             * 则 value + 1
             * 并返回 accumulator
             */
            if (accumulator.containsKey(value)) {
                Long count = accumulator.get(value);
                accumulator.put(value, count + 1);
            } else {
                accumulator.put(value, 1L);
            }
            return accumulator;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
            // 定义一个 ArrayList<Tuple2<String, Long>> 类型，因为最后输出的是 ArrayList 类型
            ArrayList<Tuple2<String, Long>> result = new ArrayList<>();
            // 获取当前的key
            for (String key : accumulator.keySet()) {
                //当获取到key之后，用 get(key) 方法获取value,有数据就加入进来
                result.add(Tuple2.of(key, accumulator.get(key)));
            }
            // 将拿到的数据使用sort方法排序
            result.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    //TODO 做降序排列，所以后面的减去前面的，又因为是long类型，所以得变成int类型相减
                    return o2.f1.intValue() - o1.f1.intValue();
                }
            });
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            return null;
        }
    }

    /**
     * ArrayList<Tuple2<String, Long>> : 输入类型
     * String : 输出类型
     * TimeWindow : time类型
     */
    //实现自定义全窗口函数，包装信息出现结果
    public static class UrlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>.Context context, Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
            // 获取每一条数据
            ArrayList<Tuple2<String, Long>> list = elements.iterator().next();
            // 定义StringBuilder 方便格式化
            StringBuilder result = new StringBuilder();
            result.append("-------------------------------\n");
            result.append("窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n");

            //去List前两个,包装信息输出
            for (int i = 0; i < 2; i++) {
                //只拿前两个数据
                Tuple2<String, Long> currTuple = list.get(i);
                String info = "No. " + (i + 1) + " "
                        + "url: " + currTuple.f0 + " "
                        + "访问量: " + currTuple.f1 + " \n";

                result.append(info);
            }

            result.append("------------------------------\n");

            out.collect(result.toString());
        }
    }
}

