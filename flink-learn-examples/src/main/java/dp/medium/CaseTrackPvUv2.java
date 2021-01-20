package dp.medium;

import bean.UMessage;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

/**
 * @Author wangwenbo
 * @Date 2020/12/11 12:08 上午
 * @Version 1.0
 * 原始数据特别大，TumblingEventTimeWindows会缓存一天数据，每次重新计算，内存吃不消啊。采用keyBy，window，重新修改这段代码。
 */



public class CaseTrackPvUv2 {
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
                        LocalDate localDate = LocalDate.parse(element.f0.getCreateTime(), TIME_FORMAT_YYYY_MM_DD_HHMMSS);
                        long timestamp = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        return timestamp;
                    }
                });
        detail.print();



        DataStream<Tuple3<String,String, Integer>> statsResult=detail
                .keyBy(new KeySelector<Tuple2<UMessage, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<UMessage, Integer> value) throws Exception {
                        return value.f0.getCreateTime().substring(0,10);
                    }
                }).window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8))) //开一天的窗口。时间可以自定义
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1))) //每秒出发一次输出
                //.trigger(ContinuousEventTimeTrigger.of(Time.seconds(1)))
                .evictor(TimeEvictor.of(Time.seconds(0), true)) //由于是全窗口,所以每促发一次会从新携带数据。可以用移除器去除
                .process(new ProcessWindowFunction<Tuple2<UMessage, Integer>, Tuple3<String,String, Integer>, String, TimeWindow>() {
                    private transient MapState<String, String> uvCountState; //保存uv 的状态 pv 可以不用管
                    private transient ValueState<Integer> pvCountState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.minutes(60 * 6)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();

                        MapStateDescriptor<String, String> uvDescriptor = new MapStateDescriptor<String, String>("uv_count", String.class, String.class);

                        ValueStateDescriptor<Integer> pvDescriptor= new ValueStateDescriptor<Integer>("pv_count", Integer.class);
                        uvDescriptor.enableTimeToLive(ttlConfig);
                        pvDescriptor.enableTimeToLive(ttlConfig);

                        uvCountState=getRuntimeContext().getMapState(uvDescriptor);
                        pvCountState=getRuntimeContext().getState(pvDescriptor);
                    }

                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<UMessage, Integer>> elements, Collector<Tuple3<String,String, Integer>> out) throws Exception {

                        Integer pv = pvCountState.value();
                        Iterator<Tuple2<UMessage, Integer>> mapIterator = elements.iterator();
                        while (mapIterator.hasNext()) {
                            if(pv == null){
                                pvCountState.update(1);
                            }else{
                                pvCountState.update(pv+1);
                            }
                            UMessage uMessage = mapIterator.next().f0;
                            String uvName = uMessage.getUid();
                            uvCountState.put(uvName,null);
                        }

                        Integer uv =0;
                        Iterator<String> uvIterator = uvCountState.keys().iterator();
                        while (uvIterator.hasNext()) {
                            uvIterator.next();
                            uv +=1;
                        }


                        out.collect(Tuple3.of(key,"pv",pvCountState.value()));
                        out.collect(Tuple3.of(key,"uv",uv));

                       /* Integer pv=0;
                        Iterator<Tuple2<UMessage,Integer>> mapIterator=elements.iterator();
                        while(mapIterator.hasNext()){
                            pv+=1;
                            UMessage uMessage=mapIterator.next().f0;
                            String uvName=uMessage.getUid();
                            uvCountState.put(uvName,null);
                        }
                        Integer uv=0;
                        Iterator<String> uvIterator=uvCountState.keys().iterator();
                        while(uvIterator.hasNext()){
                            uvIterator.next();
                            uv+=1;
                        }
                        Integer originPv=pvCountState.value();
                        if(originPv==null){
                            pvCountState.update(pv);
                        }else{
                            pvCountState.update(originPv+pv);
                        }
                        out.collect(Tuple3.of(key,"uv",uv));
                        out.collect(Tuple3.of(key,"pv",pvCountState.value()));*/
                    }
                });
        statsResult.print();



        env.execute();




    }
}
