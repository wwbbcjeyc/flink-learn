package com.wwb.state;

import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:49
 * @Version 1.0
 */
public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        stream.keyBy(data -> data.user)
                .flatMap(new MyFlatMap())
                .print();

        env.execute();

    }

    //实现自定义的FlatMapFunction 用于 Keyed State 测试
    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        //定义状态
        private ValueState<Event> myValueState; //值状态
        private ListState<Event> myListState;  // 列表状态
        private MapState<String, Long> myMapState; // 键值状态
        private ReducingState<Event> myReduceState; //规约状态
        private AggregatingState<Event, String> myAggregatingState; //聚合状态


        @Override
        public void open(Configuration parameters) throws Exception {
            //获取状态的运行上下文，方便调用状态等等。
            myValueState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("my-value", Types.POJO(Event.class)));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list", Types.POJO(Event.class)));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class, Long.class));

            myReduceState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reduce",
                    new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event value1, Event value2) throws Exception {
                            return new Event(value1.user, value1.url, value2.timestamp);
                        }
                    },
                    Types.POJO(Event.class)));

            myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("my-flatmap",
                    new AggregateFunction<Event, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Event value, Long accumulator) {
                            return accumulator + 1;
                        }

                        @Override
                        public String getResult(Long accumulator) {
                            return "count:" + accumulator;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return null;
                        }
                    },
                    Long.class));

        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //访问和更新状态
            System.out.println("原始状态:" + myValueState.value());
            myValueState.update(value);
            System.out.println("更新的状态:" + myValueState.value());

        }

        @Override
        public void close() throws Exception {
            //清除状态
            myValueState.clear();
        }
    }
}

