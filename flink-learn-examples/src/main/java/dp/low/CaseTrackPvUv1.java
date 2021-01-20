package dp.low;

import bean.UMessage;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @Author wangwenbo
 * @Date 2020/12/11 12:00 上午
 * @Version 1.0
 * windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))，从是每天凌晨开始计算的，跨天自动清空的。
 */

/**
 * {"uid":"uid001","createTime":"2020-04-08 23:50:55"}
 *
 * {"uid":"uid002","createTime":"2020-04-08 23:54:55"}
 *
 * {"uid":"uid003","createTime":"2020-04-08 23:55:55"}
 *
 * {"uid":"uid001","createTime":"2020-04-08 23:56:55"}
 *
 * {"uid":"uid001","createTime":"2020-04-09 00:01:55"}
 *
 * {"uid":"uid002","createTime":"2020-04-09 00:02:55"}
 *
 * {"uid":"uid001","createTime":"2020-04-09 00:03:55"}
 */
public class CaseTrackPvUv1 {

    public static final DateTimeFormatter TIME_FORMAT_YYYY_MM_DD_HHMMSS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception{
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
        detail.print("detail:");
        DataStream<Tuple2<String,Integer>> statsResult=
                detail.windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                        //.trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                         .trigger(CountTrigger.of(1))
                        .process(new ProcessAllWindowFunction<Tuple2<UMessage, Integer>, Tuple2<String,Integer>, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<Tuple2<UMessage, Integer>> elements, Collector<Tuple2< String, Integer>> out) throws Exception {
                                Set<String> uvNameSet=new HashSet<String>();
                                Integer pv=0;
                                Iterator<Tuple2<UMessage,Integer>> mapIterator=elements.iterator();
                                while(mapIterator.hasNext()){
                                    pv+=1;
                                    String uvName=mapIterator.next().f0.getUid();
                                    uvNameSet.add(uvName);
                                }
                                out.collect(Tuple2.of("uv", uvNameSet.size()));
                                out.collect(Tuple2.of("pv",pv));
                            }
                        });

        statsResult.print("result:");


        env.execute();


    }

}
