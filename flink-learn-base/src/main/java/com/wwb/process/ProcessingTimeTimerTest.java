package com.wwb.process;

import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:28
 * @Version 1.0
 */
public class ProcessingTimeTimerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据，并提取时间戳、生成水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {

                        // 获取当前处理时间
                        long currTs = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() + "数据到达,到达时间：" + new Timestamp(currTs));

                        //注册一个10s之后的定时器
                        ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "定时器触发,触发时间：" + new Timestamp(timestamp));

                    }
                }).print();


        env.execute();
    }

}
