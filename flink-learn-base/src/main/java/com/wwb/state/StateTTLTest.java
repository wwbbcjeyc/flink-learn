package com.wwb.state;

import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:48
 * @Version 1.0
 */
public class StateTTLTest {
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
                .flatMap(new StateTTLTest.MyFlatMap())
                .print();

        env.execute();

    }

    //实现自定义的FlatMapFunction 用于 Keyed State 测试
    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {

        //定义状态
        private ValueState<Event> myValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取状态的运行上下文，方便调用状态等等。
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("my-value", Types.POJO(Event.class));
            myValueState = getRuntimeContext().getState(valueStateDescriptor);

            //配置状态的TTL
            StateTtlConfig tlConfig = StateTtlConfig.newBuilder(Time.seconds(10)) //如果当前状态超过10s没有更新，那么就将它清除掉
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //更新的类型，(给一个更新的初始值)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //状态的可见性
                    .build();

            valueStateDescriptor.enableTimeToLive(tlConfig);

        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {

        }
    }
}
