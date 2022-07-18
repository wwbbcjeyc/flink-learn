package com.wwb.process;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:12
 * @Version 1.0
 */
public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 1.
        SingleOutputStreamOperator<JSONObject> JsonStream = env
                .socketTextStream("127.0.0.1", 9999)
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {

                        JSONObject jsonObject = JSONObject.parseObject(value);


                        return jsonObject;

                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return Long.valueOf(jsonObject.getString("event_time"));
                                    }
                                })
                );

        JsonStream.keyBy(data -> data.getString("tuid"))
                .process(new KeyedProcessFunction<String, JSONObject, String>() {


                    private transient MapState<String, String> activeState;
                    private transient ValueState<Long> countState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        activeState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>("active_map", String.class, String.class));
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                        String date =  DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now());
                        String dateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());


                        String tuid = value.getString("tuid");
                        System.out.println(tuid);

                        Long count = countState.value();


                        activeState.put("sO9nj6KXdEx_hfwyJTrxog","2021-07-26 14:36:56");


                        //状态计数并定义定时器
                        if (count == null) {
                            //第一条数据
                            countState.update(1L);
                            //注册隔天凌晨定时器
                            /*long ts = (Math.round(Long.valueOf(value.getString("event_time"))/1000L) / (60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000L) - 8 * 60 * 60 * 1000L;
                            System.out.println(ts);*/
                            ctx.timerService().registerProcessingTimeTimer(1627286521000L);

                        } else {
                            //非第一条数据
                            count = count + 1L;
                            //更新状态
                            countState.update(count);
                        }

                        System.out.println("第几条"+countState.value());

                        String activeTime = activeState.get(tuid);
                        System.out.println(activeTime);
                        if (date.equals(StringUtils.substring(activeTime, 0, 10)) && countState.value() == 2L) {

                            value.put("active_time", activeTime);
                            value.put("flag", 44);
                            out.collect(value.toJSONString());
                        }
                    }


                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println(ctx.getCurrentKey()+"触发定时器");
                        countState.clear();
                        activeState.clear();
                    }
                }).print();



        env.execute();
    }
}

