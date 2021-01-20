package com.zjtd.dp.sql;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author wangwenbo
 * @Date 2021/1/21 12:01 上午
 * @Version 1.0
 */
public class TableCubeSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("/Users/wangwenbo/Data/sensor_cube.txt");

        // 2. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], fields[1], new Long(fields[2]), new Double(fields[3]));
        });

        // 3. 创建表环
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4. 基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 5. 执行SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "SELECT id,name,sum(temperature)  FROM sensor  GROUP BY CUBE (id, name)  ";
        Table resultSqlTable = tableEnv.sqlQuery(sql);
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");
        env.execute();
    }

}
