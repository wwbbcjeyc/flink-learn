package bean;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author wangwenbo
 * @Date 2021/1/19 11:19 下午
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
