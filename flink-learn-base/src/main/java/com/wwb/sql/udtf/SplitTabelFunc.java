package com.wwb.sql.udtf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author wangwenbo
 * @Date 2022/4/24 22:16
 * @Version 1.0
 */
public class SplitTabelFunc  extends TableFunction<Row> {

    @FunctionHint(output = @DataTypeHint("ROW<str STRING>"))
    public void eval(String str,String regex) {
        String[] fields = str.split(regex);

        for(String field : fields){
           /* Row row = new Row(1);
            row.setField(0,field);
            collect(row);*/
            collect(Row.of(field));
        }
    }

}
