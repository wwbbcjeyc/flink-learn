package com.wwb.sql.udf;

import com.wwb.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author wangwenbo
 * @Date 2022/4/21 18:56
 * @Version 1.0
 */
public class UdfTest1ScalarFunction {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createDDL = "CREATE TABLE clickTable ("+
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

        tableEnv.executeSql(createDDL);

        tableEnv.createTemporarySystemFunction("MyHash",HashCode.class);

        Table resultTable = tableEnv.sqlQuery("select user_name,MyHash(user_name) from clickTable");

        tableEnv.toDataStream(resultTable).print();


        env.execute();
    }

    // 实现自定义的ScalarFunction
    public static class HashCode extends ScalarFunction {


        public int eval(String str){
            return str.hashCode() ;
        }
    }

}
