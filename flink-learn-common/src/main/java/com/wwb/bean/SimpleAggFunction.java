package com.wwb.bean;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author wangwenbo
 * @Date 2022/5/3 10:51
 * @Version 1.0
 */
public class SimpleAggFunction<T> implements AggregateFunction<T, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(T value, Long accumulator) {
        return accumulator + 1L;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}