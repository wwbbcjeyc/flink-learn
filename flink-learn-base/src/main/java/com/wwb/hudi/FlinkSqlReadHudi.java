package com.wwb.hudi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author wangwenbo
 * @Date 2022/5/22 11:19
 * @Version 1.0
 */
public class FlinkSqlReadHudi {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql(
                "CREATE TABLE read_hudi_source (\n" +
                        "    orderId STRING PRIMARY KEY NOT ENFORCED,\n" +
                        "    userId STRING,\n" +
                        "    orderTime STRING,\n" +
                        "    ip STRING,\n" +
                        "    orderPrice DOUBLE,\n" +
                        "    orderStatus INT,\n" +
                        "    ts STRING,\n" +
                        "    dt STRING\n" +
                        ")\n" +
                        "PARTITIONED BY (dt)\n" +
                        "WITH (\n" +
                        "    'connector' = 'hudi',\n" +
                        "    'path' = 'file:///D:/flink_hudi_order',\n" +
                        "    'table.type' = 'MERGE_ON_READ',\n" +
                        "    'read.streaming.enabled' = 'true',\n" +
                        "    'read.streaming.check.interval' = '4'\n" +
                        ")"
        );


        tableEnv.executeSql(
                "select * from read_hudi_source"
        );
    }
}
