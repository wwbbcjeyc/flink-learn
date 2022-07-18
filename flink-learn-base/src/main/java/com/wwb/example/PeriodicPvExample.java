package com.wwb.example;

import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author wangwenbo
 * @Date 2022/5/3 11:33
 * @Version 1.0
 */
public class PeriodicPvExample {
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

        //原始数据
        stream.print("input");

        //统计每个用户的PV，统计当前用户的访问次数，做一个累计一个聚合，输出到定时器
        stream.keyBy(data -> data.user)
                .process(new PeriodicPvResult())
                .print();

        env.execute();

    }

    //实现自定义的KeyedProcessFunction
    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {

        /**
         * 创建一个定时器，定时器，假如说我们这个周期性是10s，那就定义一个隔10s中发出结果的定时器，
         * 第一个数据来了之后，10s之后，应该要出输出一次当前的PV统计值，如果第二次输出的话，那接下来
         * 又应该定义一个定时器，所以在这个过程中，我们应该是不停的定义定时器，可以简单定一个规则，
         * 判断当前定时器是否存在，若定时器出现，一定是时间戳的形式，只要时间戳存在，则说明当前有定时器。
         */
        //定义值状态，保存当前PV值，以及有没有定时器
        ValueState<Long> countState;
        ValueState<Long> timeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取上下文
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Long.class));

        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            //每来一条数据，就更新对应的count值
            Long count = countState.value();
            countState.update(count == null ? 1 : count + 1);

            //如果没有注册过的话，注册定时器
            if(timeState.value() == null){
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timeState.update(value.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，输出一次结果
            out.collect(ctx.getCurrentKey() + " 的 pv -> count : " + countState.value());
            //触发之后，就要清空状态
            timeState.clear();
            //清空状态之后，立马再次注册
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timeState.update(timestamp + 10 * 1000L);
        }
    }
}
