package com.wwb.sql;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author wangwenbo
 * @Date 2022/4/21 17:52
 * @Version 1.0
 */
public class TopNExample{

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createDDL = "CREATE TABLE clickTable ("+
                " `user` STRING, " +
                " url STRING, " +
                " ts BIGINT ," +
                " et as TO_TIMESTAMP(FROM_UNIXTIME(ts /1000) ), " +
                " WATERMARK FOR et as et -INTERVAL '1' SECOND" +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = '/Users/wangwenbo/Data/clicks.txt', " +
                " 'format' = 'csv' " +
                ")";

        tableEnv.executeSql(createDDL);

        //普通topN 选取当前所有用户中浏览量最大的2个
      Table topNResult = tableEnv.sqlQuery("select user,cnt,row_num \n" +
                "from (\n" +
                "      select *,row_number() over( order by cnt desc ) as row_num \n" +
                "      from (select user,count(url) as cnt from clickTable group by user)\n" +
                "     ) where row_num<=2");

      //tableEnv.toChangelogStream(topNResult).print("topn:");



        //统计一段时间内的前两名用户

        String subQuery = "select user" +
                ",count(url) as cnt " +
                ",window_start,window_end " +
                " from TABLE ( " +
                " TUMBLE(TABLE clickTable,DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ") " +
                " GROUP BY user,window_start,window_end";


        Table windowTopNResult =   tableEnv.sqlQuery("SELECT user,cnt,row_num " +
                " FROM ( " +
                "      SELECT *,ROW_NUMBER() OVER ( " +
                "      ORDER BY cnt DESC " +
                "     ) as row_num " +
                "      FROM (" + subQuery + " )" +
                "     ) WHERE row_num <= 2 ");

        tableEnv.toChangelogStream(windowTopNResult).print("window top:");




        env.execute();
    }
}
