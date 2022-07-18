package com.wwb.process;

import com.wwb.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:14
 * @Version 1.0
 */
public class ProcessFunctionTimerPractice {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 1.
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }));


        SingleOutputStreamOperator<String> processDS = sensorDS
                .keyBy(data -> data.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            // 定义一个变量，保存上一次的水位值
                            private Integer lastVC = 0;
                            private Long triggerTs = 0L;

                            /**
                             * 来一条数据，处理一条
                             * @param value
                             * @param ctx
                             * @param out
                             * @throws Exception
                             */
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 判断是上升还是下降
                                if (value.getVc() > lastVC) {
                                    // 1.水位上升
                                    if (triggerTs == 0) {
                                        // 第一条数据来的时候，注册定时器
                                        ctx.timerService().registerEventTimeTimer(value.getTs() * 1000L + 5000L);
                                        triggerTs = value.getTs() * 1000L + 5000L;
                                    }
                                } else {
                                    // 2.水位下降
                                    // 2.1 删除注册的定时器
                                    ctx.timerService().deleteEventTimeTimer(triggerTs);
                                    // 2.2 重新注册定时器（或 把保存的时间清空）
                                    triggerTs = 0L;
                                }

                                // 不管上升还是下降，都要保存水位值，供下条数据使用，进行比较
                                lastVC = value.getVc();
                            }

                            /**
                             * 定时器触发
                             * @param timestamp 注册的定时器的时间
                             * @param ctx   上下文
                             * @param out   采集器
                             * @throws Exception
                             */
                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                // 定时器触发，说明已经满足 连续5s 水位上升
                                out.collect(ctx.getCurrentKey() + "在"+timestamp+"监测到水位连续5s上升");
                                // 将保存的注册时间清空
                                triggerTs = 0L;
                            }
                        }
                );


        processDS.print();

        env.execute();
    }
}

