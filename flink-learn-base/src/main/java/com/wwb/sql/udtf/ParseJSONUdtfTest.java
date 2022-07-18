package com.wwb.sql.udtf;

import com.wwb.sql.udf.GetJsonObject;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author wangwenbo
 * @Date 2022/4/24 22:53
 * @Version 1.0
 */
public class ParseJSONUdtfTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceDDL = ""
                + "create table source_kafka "
                + "( "
                + "    json_str String "
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

        //tableEnv.createTemporarySystemFunction("parse_json",ParseJson.class);
        tableEnv.createTemporarySystemFunction("get_json_object", GetJsonObject.class);

        //fsTableEnv.registerFunction("json_value1",new GetJsonObject());


        //Table resultTable = tableEnv.sqlQuery("select * from source_kafka");

        /*Table resultTable = tableEnv.sqlQuery("select T.arr[1] " +
                        "    from source_kafka a " +
                        "    LEFT JOIN LATERAL TABLE(parse_json(json_str, 'user_id')) AS T(arr) ON TRUE");*/


               /* Table resultTable = tableEnv.sqlQuery("select T.arr[1] ,T1.arr[1], T1.arr[2]\n" +
                        "    from source_kafka a \n" +
                        "    LEFT JOIN LATERAL TABLE(parse_json(json_str, 'user_id','sub_json')) AS T(arr) ON TRUE \n" +
                        "    LEFT JOIN LATERAL TABLE(parse_json(T.arr[2], 'username' , 'password')) AS T1(arr) ON TRUE"

                 );*/

        Table resultTable = tableEnv.sqlQuery("select get_json_object(json_str,'$.category_id') as category_id" +
                ",get_json_object(json_str,'$.user_id') " +
                ",get_json_object(json_str,'$.item_id') " +
                ",get_json_object(json_str,'$.sub_json.password')" +
                " from source_kafka");
        tableEnv.toDataStream(resultTable).print();



        env.execute();


    }
}
