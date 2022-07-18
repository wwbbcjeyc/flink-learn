package com.wwb.hudi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.*;
/**
 * @Author wangwenbo
 * @Date 2022/5/22 10:12
 * @Version 1.0
 */
public class FlinkSqlKafkaSinkHudi {

    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);
        /*
        {
          "orderId":"20220518104723336000001",
           "userId":"1001",
           "orderTime":"2022-05-18 10:47:23.336",
           "ip":"123.232.118.98",
           "price":"324.47",
           "status":0 
         }
         */
        tableEnv.executeSql(
                "CREATE TABLE order_kafka_source (\n" +
                        "    orderId STRING,\n" +
                        "    userId STRING,\n" +
                        "    orderTime STRING,\n" +
                        "    ip STRING,\n" +
                        "    price DOUBLE,\n" +
                        "    status INT \n" +
                        ") WITH (\n" +
                        "    'connector' = 'kafka',\n" +
                        "    'topic' = 'order topic',\n" +
                        "    'properties.bootsstrap.servers' = 'node1.kafka.cn:9092',\n" +
                        "    'properties.group.id' = 'gid-1001',\n" +
                        "    'scan.startup.mode' = 'latest-offset',\n" +
                        "    'format' = 'json',\n" +
                        "    'json.fail-on-miissing-field' = 'false',\n" +
                        "    'json.ignore-parse-errors' = 'true'\n" +
                        ");"
        );

        Table etlTable = tableEnv
                .from("abTestKafkaSource")
                .addColumns(
                        $("orderTime").substring(0, 10).as("dt")
                )
                .addColumns(
                        $("orderId").substring(0, 17).as("ts")
                );

        tableEnv.createTemporaryView("view_order",etlTable);

        //tableEnv.executeSql("select * from view_order").print();

        tableEnv.executeSql(
                "CREATE TABLE order_hudi_sink (\n" +
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
                        "    'write.operation' = 'upsert',\n" +
                        "    'hoodie.datasource.write.recordkey.field' = 'orderId',\n" +
                        "    'write.precombine.field' = 'ts',\n" +
                        "    'write.tasks' = '1'\n" +
                        ")"
        );

        tableEnv.executeSql(
                "INSERT INTO order_hudi_sink " +
                        "SELECT orderId,userId,orderTime,ip,price,status,ts,dt FROM view_order "
        );

    }
}
