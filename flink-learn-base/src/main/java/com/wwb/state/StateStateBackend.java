package com.wwb.state;

import com.wwb.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:17
 * @Version 1.0
 */
public class StateStateBackend {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // TODO env设置 状态后端
        // 1.设置为 MemoryStateBackend
        // 1.1 创建一个 MemoryStateBackend对象
        StateBackend memoryStateBackend = new MemoryStateBackend();
        // 1.2 env中指定
        env.setStateBackend(memoryStateBackend);

        // 2.设置为FsStateBackend
        // 2.1 创建 状态后端 对象
        StateBackend fsStateBackend = new FsStateBackend("hdfs://xxxx:xx/flink/statebackend/");
        env.setStateBackend(fsStateBackend);

        // 3. 设置为 RocksDBStateBackend =》 需要先引入 依赖
        // 3.1 创建 状态后端 对象
        StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://xxxx:xx/flink/statebackend/rocksdb/");
        env.setStateBackend(rocksDBStateBackend);

        // TODO 开启 checkpoint
        env.enableCheckpointing(3000L);

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
                .assignTimestampsAndWatermarks(
                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {
                            private Long maxTs = Long.MIN_VALUE;

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                                maxTs = Math.max(maxTs, extractedTimestamp);
                                return new Watermark(maxTs);
                            }

                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }
                );


        SingleOutputStreamOperator<String> processDS = sensorDS
                .keyBy(data -> data.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            // 定义一个变量，保存上一次的水位值
                            private ValueState<Integer> lastVC;
                            private ValueState<Long> triggerTs;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                lastVC = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVC", Integer.class, 0));
                                triggerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTs", Long.class));
                            }

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
                                if (value.getVc() > lastVC.value()) {
                                    // 1.水位上升
                                    if (triggerTs.value() == null) {
                                        // 第一条数据来的时候，注册定时器
                                        ctx.timerService().registerEventTimeTimer(value.getTs() * 1000L + 5000L);
                                        triggerTs.update(value.getTs() * 1000L + 5000L);
                                    }
                                } else {
                                    // 2.水位下降
                                    // 2.1 删除注册的定时器
                                    ctx.timerService().deleteEventTimeTimer(triggerTs.value());
                                    // 2.2 重新注册定时器（或 把保存的时间清空）
                                    triggerTs.clear();
                                }

                                // 不管上升还是下降，都要保存水位值，供下条数据使用，进行比较
                                lastVC.update(value.getVc());
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
                                out.collect(ctx.getCurrentKey() + "在" + timestamp + "监测到水位连续5s上升,当前watermark=" + ctx.timerService().currentWatermark()
                                        + ",当前保存水位的状态=" + lastVC.value());
                                // 将保存的注册时间清空
                                triggerTs.clear();
                            }
                        }
                );


        processDS.print();

        env.execute();
    }
}

