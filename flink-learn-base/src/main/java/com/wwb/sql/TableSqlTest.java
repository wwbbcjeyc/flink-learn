package com.wwb.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author wangwenbo
 * @Date 2022/4/21 20:10
 * @Version 1.0
 */
public class TableSqlTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceDDL = ""
                + "create table source_kafka "
                + "( "
                + "    deviceid String, "
                + "    cde_sensors_event String, "
                + "    experimentid String, "
                + "    classid String, "
                + "    ts      String, "
                + "    proctime AS PROCTIME() "
                + ") with ( "
                + "    'connector' = 'kafka', "
                + "    'topic' = 'first', "
                + "    'properties.bootstrap.servers' = 'localhost:9092', "
                + "    'properties.group.id' = 'wwb01', "
                + "    'scan.startup.mode' = 'latest-offset', "
                + "    'json.ignore-parse-errors' = 'true',"
                + "    'format' = 'json' "
                + ")";

        tableEnv.executeSql(sourceDDL);


        String execSQL = "select \n" +
                "     ts\n" +
                "    ,deviceid\n" +
                "    ,experimentid\n" +
                "    ,classid\n" +
                "from \n" +
                "(\n" +
                "    select \n" +
                "         ts\n" +
                "        ,deviceid\n" +
                "        ,experimentid\n" +
                "        ,classid\n" +
                "        ,ROW_NUMBER() over (partition by deviceid,experimentid order by remark desc,ts desc) as rnk\n" +
                "    from \n" +
                "    (\n" +
                "        select \n" +
                "             deviceid, 2 as remark, experimentid, classid,ts\n" +
                "        from source_kafka\n" +
                "        where cde_sensors_event = 'abtestingByService'\n" +
                "        union all \n" +
                "        select \n" +
                "            deviceid, 1 as remark, experimentid, classid,ts\n" +
                "        from source_kafka\n" +
                "        where cde_sensors_event = 'abtesting'\n" +
                "    ) a\n" +
                ") b\n" +
                "where rnk = 1";


        Table resultTable = tableEnv.sqlQuery(execSQL);

        tableEnv.toChangelogStream(resultTable).print();


        env.execute("sql job test");
    }
}
