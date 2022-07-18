package com.wwb.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author wangwenbo
 * @Date 2022/4/20 22:10
 * @Version 1.0
 */
public class CommonApiTest1 {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //定义环境配置创建表执行环境
        //1.1 用Blink planner 进行流处理
        EnvironmentSettings settings1 = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv1 = TableEnvironment.create(settings1);



      /*  //1.2 用老版本planner 进行批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);

        //1.3 基于blink 版本planner 进行批处理
        EnvironmentSettings settings3 = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();
        TableEnvironment tableEnv3 = TableEnvironment.create(settings3);*/

         //default_catalog.default_database.MyTable
        /* tableEnv1.useCatalog("custom_catalog");
         tableEnv1.useCatalog("custom_database");*/

        //2. 创建表
        String createDDL = "CREATE TABLE clickTable ("+
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = '/Users/wangwenbo/Data/clicks.txt', " +
                " 'format' = 'csv' " +
                ")";

        tableEnv1.executeSql(createDDL);

        Table clickTable = tableEnv1.from("clickTable");
        Table resultTable1 = clickTable.where($("user_name").isEqual("Bob"))
                .select($("user_name"), $("url"));
        tableEnv1.createTemporaryView("result1",resultTable1);


        //执行sql 进行查询转换

       Table resultTable2 =  tableEnv1.sqlQuery("select url,user_name from result1");

       //执行聚合计算
        Table aggResult = tableEnv1.sqlQuery("select user_name,count(url) as cnt from clickTable group by user_name");

        // 创建输出表
        String createOutDDL = "CREATE TABLE outTable  ("+
                " url STRING, " +
                " user_name STRING " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = '/Users/wangwenbo/Data/out/', " +
                " 'format' = 'csv' " +
                ")";
        tableEnv1.executeSql(createOutDDL);

        //创建控制台打印表

        String creatPrintDDL = "CREATE TABLE printOutTable  ("+
                " user_name STRING, " +
                " cnt BIGINT " +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";
        tableEnv1.executeSql(creatPrintDDL);



        //输出表
        //resultTable2.executeInsert("outTable");
        //resultTable2.executeInsert("printOutTable");

        aggResult.executeInsert("printOutTable");







       // env.execute();

    }
}
