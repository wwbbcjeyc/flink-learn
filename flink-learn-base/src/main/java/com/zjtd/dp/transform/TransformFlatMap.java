package com.zjtd.dp.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * @Author Wang wenbo
 * @Date 2021/1/20 11:03
 * @Version 1.0
 */
public class TransformFlatMap {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.从文件读取数据
        DataStreamSource<List<Integer>> inputDS = env.fromCollection(
                Arrays.asList(
                        Arrays.asList(1, 2, 3, 4),
                        Arrays.asList(5, 6, 7, 8)
                )
        );

        // 2.Transform: FlatMap转换成实体对象
        inputDS
                .flatMap(new FlatMapFunction<List<Integer>, Integer>() {
                    @Override
                    public void flatMap(List<Integer> value, Collector<Integer> out) throws Exception {
                        for (Integer number : value) {
                            out.collect(number + 10);
                        }
                    }
                })
                .print();


        env.execute();
    }
}
