package com.wwb.sql;

import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author wangwenbo
 * @Date 2022/6/17 12:08
 * @Version 1.0
 */
public class SimpleTableExample02 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            public long extractTimestamp(Event element, long recordTimeStamp) {
                                return element.timestamp;
                            }
                        }));

        EnvironmentSettings settings1 = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings1);


        Table eventTable = tableEnv.fromDataStream(eventStream);
        tableEnv.createTemporaryView("clickTable",eventTable);

        Table resultTable1 = tableEnv.sqlQuery("select user,url,`timestamp` from clickTable ");

        tableEnv.toDataStream(resultTable1).print("数据源：");


        Table Aliceresult = tableEnv.sqlQuery("select user,url,`timestamp` from clickTable where user ='Alice' ");

        tableEnv.toChangelogStream(Aliceresult).print("过滤Alice:");

        Table Caryresult = tableEnv.sqlQuery("select user,url,`timestamp` from clickTable where user ='Cary'");

        tableEnv.toChangelogStream(Caryresult).print("过滤Cary:");

        env.execute();
    }
}
