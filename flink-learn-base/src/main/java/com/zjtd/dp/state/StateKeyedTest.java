package com.zjtd.dp.state;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * @Author wangwenbo
 * @Date 2020/12/14 11:04 下午
 * @Version 1.0
 */

/**
 * sensor_1,1549065725,10
 * sensor_1,1549065726,20
 * sensor_1,1549065727,30
 * sensor_1,1549065728,40
 * sensor_2,1549065729,50
 * sensor_1,1549065730,60
 * sensor_1,1549065731,70
 * sensor_2,1549065732,80
 * sensor_1,1549065733,90
 * sensor_1,1549065734,100
 * sensor_3,1549065735,110
 * sensor_3,1549065736,120
 *
 * sensor_1,1549123322,30
 *
 * sensor_1,1549152122,30
 * sensor_1,1549152123,30
 * sensor_2,1549152124,60
 * sensor_3,154915215,90
 */

public class StateKeyedTest {

    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 1.env指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        SingleOutputStreamOperator<String> filterDS = sensorDS.keyBy(data -> data.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            //定义变量，保存最开始的时间数据
                            private MapState<String, Long> firstMapState;
                            //保存key的cnt
                            private ValueState<Long> cntState;

                            private Long triggerTs = 0L;

                            OutputTag<String> alarm = new OutputTag<String>("blacklist") {
                            };


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                firstMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("firstMapState", String.class, Long.class));
                                cntState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("cntState", Long.class,0L));
                            }

                            @Override
                            public void processElement(WaterSensor waterSensor, Context ctx, Collector<String> out) throws Exception {

                                if(triggerTs ==0){
                                    long currentDays = ctx.timestamp() / (24 * 60 * 60 * 1000L);
                                    long nextDays = currentDays + 1;
                                    long nextDayTs = nextDays * (24 * 60 * 60 * 1000L);
                                    ctx.timerService().registerEventTimeTimer(nextDayTs);
                                    triggerTs=nextDayTs;
                                }

                                String lastId = waterSensor.getId();
                                Long lastTs = waterSensor.getTs();
                                Long firstTs = firstMapState.get(lastId);
                                Long currentClickCount = cntState.value();

                               if(currentClickCount==0){
                                   cntState.update(currentClickCount+1);
                                   firstMapState.put(lastId,lastTs);
                                   out.collect("当前数据所属的key=" + waterSensor.getId() + ",当前累积数值是=" + cntState.value() + "当前时间=" + waterSensor.getTs()+ ",最初时间=" + firstMapState.get(waterSensor.getId()));
                               }else{
                                   if(lastTs-firstTs<=5){
                                       cntState.update(currentClickCount +1);
                                       out.collect("当前数据所属的key=" + waterSensor.getId() + ",当前累积数值是=" + cntState.value() + "当前时间=" + waterSensor.getTs()+ ",最初时间=" + firstMapState.get(waterSensor.getId()));
                                   }

                               }



                            }


                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);
                                System.out.printf("清理前：当前key:"+ctx.getCurrentKey()+",当前cnt:"+cntState.value()+",当前map:"+firstMapState.isEmpty());
                                cntState.clear();
                                firstMapState.clear();
                                triggerTs=0L;
                                System.out.printf("清理后：当前key:"+ctx.getCurrentKey()+",当前cnt:"+cntState.value()+",当前map:"+firstMapState.isEmpty());
                            }

                        }


                );

        filterDS.print("filterDS:");

        env.execute("StateKeyedTest");
    }
}
