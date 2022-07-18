package com.wwb.sql.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * @Author wangwenbo
 * @Date 2022/4/21 19:06
 * @Version 1.0
 */
public class SplitFunctionTableFuncTest{

    public static void main(String[] args) throws Exception {

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


        tableEnv.createTemporarySystemFunction("SplitFunction",SplitFunction.class);
      //  tableEnv.createTemporarySystemFunction("SplitFunction1",SplitFunction1.class);
       // tableEnv.createTemporarySystemFunction("splitarr", SplitArr.class);


        Table resultTable = tableEnv.sqlQuery("select user_name,url,word,length " +
                "from clickTable, LATERAL TABLE(SplitFunction(url)) as T(word,length)");

       /* Table resultTable1 = tableEnv.sqlQuery("select user_name,info,str " +
                "from clickTable1, LATERAL TABLE(SplitFunction1(info)) as T(str)");*/
       // Table resultTable1 = tableEnv.sqlQuery("select * from clickTable1");

        tableEnv.toDataStream(resultTable).print();

        env.execute();

    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING,length INT>"))
    public static class SplitFunction extends TableFunction<Row>{
        public void eval(String str) {
            for(String s : str.split("\\?")){
                collect(Row.of(s,s.length()));
            }
        }
    }

    @FunctionHint(output = @DataTypeHint("ROW<str STRING>"))
    public static class SplitFunction1 extends TableFunction<Row>{
        public void eval(String str) {
            for(String st : str.split("\\,")){
                collect(Row.of(st));
            }
        }
    }

    @FunctionHint(output = @DataTypeHint("ROW<content_type STRING,url STRING>"))
    public static class ParserJsonArrayTest extends TableFunction<Row> {

        private static final Logger log = Logger.getLogger(ParserJsonArrayTest.class);

        public void eval(String value) {
            try {
                JSONArray snapshots = JSONArray.parseArray(value);
                Iterator<Object> iterator = snapshots.iterator();
                while (iterator.hasNext()) {
                    JSONObject jsonObject = (JSONObject) iterator.next();
                    String content_type = jsonObject.getString("content_type");
                    String url = jsonObject.getString("url");
                    collect(Row.of(content_type,url));
                }
            } catch (Exception e) {
                log.error("parser json failed :" + e.getMessage());
            }
        }
    }

    public static class SplitArr extends ScalarFunction {

        public String[] eval(String str){
            return str.split("\\,");
        }
    }


}


