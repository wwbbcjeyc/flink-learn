package com.wwb.sql.udtf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * @Author wangwenbo
 * @Date 2022/4/25 11:23
 * @Version 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<content_type STRING,url STRING>"))
public class ParserJsonArrayFunction extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(ParserJsonArrayFunction.class);

    public void eval(String value) {
        try {
            JSONArray jsonArray = JSONArray.parseArray(value);
            Iterator<Object> iterator = jsonArray.iterator();
            while (iterator.hasNext()) {
                JSONObject jsonObject = (JSONObject) iterator.next();
                String content_type = jsonObject.getString("content_type");
                String url = jsonObject.getString("url");
                collect(Row.of(content_type, url));
            }
        } catch (Exception e) {
            LOG.error("parser json failed :" + e.getMessage());
        }
    }
}
