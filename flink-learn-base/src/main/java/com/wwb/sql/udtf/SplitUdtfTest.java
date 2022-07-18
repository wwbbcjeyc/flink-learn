package com.wwb.sql.udtf;

import akka.stream.impl.fusing.Split;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author wangwenbo
 * @Date 2022/4/24 14:45
 * @Version 1.0
 */
public class SplitUdtfTest {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceDDL = ""
                + "create table source_kafka "
                + "( "
                + "    distinctId String, "
                + "    isLogin Bigint, "
                + "    event String, "
                + "    segmentABResult String, "
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

        tableEnv.createTemporarySystemFunction("SplitTabelFunc",SplitTabelFunc.class);


        Table resultTable = tableEnv.sqlQuery("select distinctId,str " +
                ",SPLIT_INDEX(str,']_',0) as f0 " +
                ",SPLIT_INDEX(str,']_',1) as f1 " +
                ",SPLIT_INDEX(str,']_',2) as f2 " +
                ",SPLIT_INDEX(str,']_',3) as f3 " +
                ",SPLIT_INDEX(str,']_',4) as f4 " +
                " from source_kafka ,lateral TABLE (SplitTabelFunc(segmentABResult,',')) AS t (str)");
        tableEnv.toDataStream(resultTable).print();



        env.execute();
    }





}
