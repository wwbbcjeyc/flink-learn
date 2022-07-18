package com.wwb.cep;

import com.wwb.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 23:33
 * @Version 1.0
 */
public class CEPAPI03 {

    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.从文件读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("/Users/wangwenbo/Data/sensor-data-cep.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
//                        new AscendingTimestampExtractor<WaterSensor>() {
//                            @Override
//                            public long extractAscendingTimestamp(WaterSensor element) {
//                                return element.getTs() * 1000L;
//                            }
//                        }
                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {
                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                                return new Watermark(extractedTimestamp);
                            }

                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        // TODO 使用 CEP
        // 1.定义规则
        // times(m,n) =》 m次 到 n次 都可以 => (1,5) =》 1、2、3、4、5次都可以
        // within(Time) => 时间限制 => 不包括 Time这个值
        //time 作用范围：当前事件=>之后 事件 * 次数 当作一个整体 =》与其他事件产生联系(next followed...)
        //1+ (1*2) =>先括号里的1*2=2  =》然后2与前面的1相加
        //times 类似宽松近邻的关系
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                })
                .next("next")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                })
//                .times(1,3)
                .within(Time.seconds(3));

        // 2.使用规则
        PatternStream<WaterSensor> sensorPS = CEP.pattern(sensorDS, pattern);

        // 3.取出匹配的结果
        SingleOutputStreamOperator<String> selectDS = sensorPS.select(
                new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> map) throws Exception {
                        String start = map.get("start").toString();
                        String next = map.get("next").toString();
                        return start + "-->" + next;
//                        return start;
                    }
                }
        );

        selectDS.print("cep");

        env.execute();
    }

}

