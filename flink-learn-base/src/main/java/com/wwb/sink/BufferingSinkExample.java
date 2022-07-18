package com.wwb.sink;


import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
/**
 * @Author wangwenbo
 * @Date 2022/5/2 22:26
 * @Version 1.0
 */
public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        stream.print("input");

        //批量缓存输出 (攒到10条数据就做一个输出)
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    //自定义实现SinkFunction

    /**
     * 假如说如果没到10条数据，但已经发生了故障，那么发生故障后，之前的数据就已经消失了，
     * 为了保证数据不丢，那么就要做一个持久化的缓存，就是所谓的检查点
     */
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {

        //定义当前类的属性，批量
        private final int threshold;

        //将数据写入持久化,需要媒介,定义一个List集合,充当媒介
        private List<Event> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        //定义算子状态
        private ListState<Event> checkpointedState;

        //每来一条数据，要做什么操作，都在这个方法里
        @Override
        public void invoke(Event value, Context context) throws Exception {
            //把来的每一条数据都缓存到列表中
            bufferedElements.add(value);
            //判断如果达到阈值，就批量写入
            if (bufferedElements.size() == threshold) {
                //用打印到控制台模拟写入外部系统
                for (Event event : bufferedElements) {
                    System.out.println(event);
                }
                System.out.println("====================输出完毕====================");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            //清空状态，保证状态跟这里的bufferedElements完全一样
            checkpointedState.clear();

            //对状态进行持久化，复制缓存的列表到列表状态
            for (Event event : bufferedElements) {
                checkpointedState.add(event);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffer", Event.class);
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            //如果故障恢复，需要将ListState中的所有元素复制到列表中
            if (context.isRestored()) {
                for (Event event : checkpointedState.get()){
                    bufferedElements.add(event);
                }
            }
        }
    }
}

