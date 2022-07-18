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
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author wangwenbo
 * @Date 2022/4/20 21:31
 * @Version 1.0
 */
public class SimpleTableExample {

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

        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings1);


        //流转表
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // 第一种 sql 进行转换
        Table resultTable1 = tableEnv.sqlQuery("select user,url,`timestamp` from " + eventTable);


       // tableEnv.toDataStream(resultTable1).print("result1");

        //第二种 table 进行转换
        Table resultTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));

        //tableEnv.toDataStream(resultTable2).print("result2");

        tableEnv.createTemporaryView("clickTable",eventTable);
        //7.聚合转换
        Table aggResult = tableEnv.sqlQuery("select user,count(url) as cnt from clickTable group by user");

        tableEnv.toChangelogStream(aggResult).print("agg");
        //tableEnv.toRetractStream();

        tableEnv.sqlQuery("select " +
                                "user," +
                                "window_end as endT," +
                                "count(url) as cnt " +
                          "from clickTable(" +
                               "TUMBLE(" +
                                        "TABLE clickTable," +
                                        "DESCRIPTOR(ts)," +
                                        "INTERVAL '1' HOUR)) " +
                                        "group by user," +
                                        "windows_start,window_end"
                          );

        env.execute();



    }
}
