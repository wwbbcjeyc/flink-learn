package com.wwb.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author wangwenbo
 * @Date 2022/4/20 20:35
 * @Version 1.0
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> lineDataSource = env.readTextFile("/Users/wangwenbo/Data/word.txt");


        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {

            String[] words = line.split(" ");

            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }

        }).returns(Types.TUPLE(Types.STRING,Types.LONG));

        UnsortedGrouping<Tuple2<String, Long>> wordAndOneKS = wordAndOneTuple.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        result.print();


    }
}
