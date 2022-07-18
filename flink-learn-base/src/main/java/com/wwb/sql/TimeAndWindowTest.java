package com.wwb.sql;

import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author wangwenbo
 * @Date 2022/4/21 14:59
 * @Version 1.0
 */
public class TimeAndWindowTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //状态生存时间
        //获取表环境配置
       // TableConfig tableConfig = tableEnv.getConfig();
        //配置状态保持时间
       // tableConfig.setIdleStateRetention(Duration.ofMinutes(60));

        //也可以设置配置项 table.exec.state.ttl
       /* Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl","60 min");*/



        //创建DDLl 中定义时间属性 事件时间
        String createDDL1 = "CREATE TABLE clickTable ("+
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT ," +
                " et as TO_TIMESTAMP(FROM_UNIXTIME(ts /1000) ), " +
                " WATERMARK FOR et as et -INTERVAL '1' SECOND" +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = '/Users/wangwenbo/Data/clicks.txt', " +
                " 'format' = 'csv' " +
                ")";

        tableEnv.executeSql(createDDL1);

        //在流转表时候定义时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimeStamp) {
                                return element.timestamp;
                            }
                        }));

        Table clickTable = tableEnv.fromDataStream(clickStream, $("user")
                , $("url"), $("timestamp").as("ts"), $("et").rowtime());


        //分组聚合
        Table aggTable =   tableEnv.sqlQuery("select user_name,count(1) from clickTable group by user_name");
       // tableEnv.toChangelogStream(aggTable).print("agg");

       //分组窗口聚合 老版本
      Table groupWindowTable =   tableEnv.sqlQuery("select " +
                "user_name,count(1) as cnt, " +
                "TUMBLE_END(et,INTERVAL '10' SECOND) as entT " +
                "from clickTable " +
                "group by user_name, " +
                "TUMBLE(et,INTERVAL '10' SECOND)");

        //tableEnv.toChangelogStream(groupWindowTable).print("Groupagg");

        //窗口聚合
          //滚动聚合
      Table tumbleWindowResult =   tableEnv.sqlQuery("select user_name,count(1) as cnt , " +
                " window_end as endT " +
                " from TABLE( " +
                " TUMBLE(TABLE clickTable,DESCRIPTOR(et),INTERVAL '10' SECOND) " +
                ") " +
                "GROUP by user_name,window_end,window_start");
         // tableEnv.toChangelogStream(tumbleWindowResult).print("Tumble");

        //滑动聚合
        Table hopWindowResult =   tableEnv.sqlQuery("select user_name,count(1) as cnt , " +
                " window_end as endT " +
                " from TABLE( " +
                " TUMBLE(TABLE clickTable,DESCRIPTOR(et),INTERVAL '5' SECOND,INTERVAL '10' SECOND) " +
                ") " +
                "GROUP by user_name,window_end,window_start");
       // tableEnv.toChangelogStream(hopWindowResult).print("hop");




        //开窗聚合over
        Table overWindowResultTable = tableEnv.sqlQuery("select user_name, " +
                "avg(ts) OVER( " +
                " PARTITION BY user_name " +
                " ORDER BY et " +
                " ROWS BETWEEN 3 PRECEDING AND CURRENT ROW " +
                ") as avg_ts " +
                " FROM clickTable ");
      //  tableEnv.toChangelogStream(overWindowResultTable).print("over window:");


        //row_number

       /* Table rowNUmberTable = tableEnv.sqlQuery(
                "select user_name,url,ts,row_num \n" +
                "from (\n" +
                "      select * \n" +
                "            ,row_number()over(partition by user_name,order by char_length(url) desc) as row_num\n" +
                "     from clickTable\n" +
                "     )\n" +
                "where row_num<=2");
         tableEnv.toChangelogStream(rowNUmberTable).print("row_number:");*/



        //clickTable.printSchema();
        env.execute();



    }
}
