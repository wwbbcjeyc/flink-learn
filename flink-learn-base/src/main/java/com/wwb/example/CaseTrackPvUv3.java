package com.wwb.example;


import com.alibaba.fastjson.JSON;
import com.wwb.bean.UMessage;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

/**
 * @Author wangwenbo
 * @Date 2020/12/11 12:09 上午
 * @Version 1.0
 * UV如果很多，MapState太大了，而且要每次遍历。采用BoomFilter
 */
public class CaseTrackPvUv3 {
    public static final DateTimeFormatter TIME_FORMAT_YYYY_MM_DD_HHMMSS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public static void main(String[] args)throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);

        final SingleOutputStreamOperator<Tuple2<UMessage, Integer>> detail = streamSource.map(new MapFunction<String, Tuple2<UMessage, Integer>>() {
            @Override
            public Tuple2<UMessage, Integer> map(String value) throws Exception {
                try {
                    UMessage uMessage = JSON.parseObject(value, UMessage.class);
                    return Tuple2.of(uMessage, 1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return Tuple2.of(null, null);
            }
        }).filter(s -> s != null && s.f0 != null)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<UMessage, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<UMessage, Integer> element) {

                        LocalDateTime localDateTime = LocalDateTime.parse(element.f0.getCreateTime(), TIME_FORMAT_YYYY_MM_DD_HHMMSS);

                        long timestamp = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();

                        return timestamp;
                    }
                });
        detail.print();

        DataStream<Tuple3<String, String, Integer>> statsResult = detail.keyBy(new KeySelector<Tuple2<UMessage, Integer>, String>() {
            @Override
            public String getKey(Tuple2<UMessage, Integer> value) throws Exception {
                return value.f0.getCreateTime().substring(0, 10);
            }
        }).window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                //.trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(1)))
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                .process(new ProcessWindowFunction<Tuple2<UMessage, Integer>, Tuple3<String, String, Integer>, String, TimeWindow>() {


                    private transient ValueState<BloomFilter<String>> boomFilterState;
                    private transient ValueState<Integer> uvCountState;
                    private transient ValueState<Integer> pvCountState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.minutes(60 * 6))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 仅在创建和写入时更新
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //不返回过期数据
                                .build();

                        ValueStateDescriptor<BloomFilter<String>> boomFilterDescriptor = new ValueStateDescriptor<BloomFilter<String>>("boom_filter", TypeInformation.of(new TypeHint<BloomFilter<String>>() {
                        }));
                        ValueStateDescriptor<Integer> pvDescriptor = new ValueStateDescriptor<Integer>("pv_count", Integer.class);
                        ValueStateDescriptor<Integer> uvDescriptor = new ValueStateDescriptor<Integer>("uv_count", Integer.class);
                        boomFilterDescriptor.enableTimeToLive(ttlConfig);
                        pvDescriptor.enableTimeToLive(ttlConfig);
                        uvDescriptor.enableTimeToLive(ttlConfig);
                        boomFilterState = getRuntimeContext().getState(boomFilterDescriptor);
                        pvCountState = getRuntimeContext().getState(pvDescriptor);
                        uvCountState = getRuntimeContext().getState(uvDescriptor);
                    }

                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<UMessage, Integer>> elements, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        Integer uv = uvCountState.value();
                        Integer pv = pvCountState.value();
                        BloomFilter<String> bloomFilter = boomFilterState.value();
                        if (bloomFilter == null) {
                            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 1<<19);
                            uv = 0;
                            pv = 0;
                        }

                        Iterator<Tuple2<UMessage, Integer>> mapIterator = elements.iterator();
                        while (mapIterator.hasNext()) {
                            pv += 1;
                            UMessage uMessage = mapIterator.next().f0;
                            String uid = uMessage.getUid();
                            if (!bloomFilter.mightContain(uid)) {
                                bloomFilter.put(uid); //不包含就添加进去
                                uv += 1;
                            }
                        }

                        boomFilterState.update(bloomFilter);
                        uvCountState.update(uv);
                        pvCountState.update(pv);

                        out.collect(Tuple3.of(key, "uv", uv));
                        out.collect(Tuple3.of(key, "pv", pv));

                    }
                });
        statsResult.print();





        env.execute();




    }
}
