package com.wwb.example;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author wangwenbo
 * @Date 2022/5/3 11:22
 * @Version 1.0
 */
public class BehaviorPatternDetectExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //用户行为数据流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "order")
        );

        //行为模式流，基于它构建广播流
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "order")
        );

        //定义广播状态描述器
        MapStateDescriptor<Void,Pattern> descriptor = new MapStateDescriptor<Void,Pattern>("pattern", Types.VOID,Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(descriptor);

        //连接两条流进行处理
        SingleOutputStreamOperator<Tuple2<String,Pattern>> matches = actionStream.keyBy(data -> data.userId)
                .connect(broadcastStream)
                .process(new PatterDetector());

        matches.print();

        env.execute();

    }

    //实现自定义的KeyedBroadcastProcessFunction
    public static class PatterDetector extends KeyedBroadcastProcessFunction<String,Action,Pattern,Tuple2<String,Pattern>>{

        //定义一个KeyedState 保存上一次用户行为
        public ValueState<String> prevActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-action",String.class));
        }

        @Override
        public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 从广播状态中匹配模式 (只读状态)
            ReadOnlyBroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<Void, Pattern>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern = patternState.get(null); //????

            // 获取用户上一次的行为
            String prevAction = prevActionState.value();

            //判断是否匹配
            if (pattern != null && prevAction != null){
                if (pattern.action1.equals(prevAction) && pattern.action2.equals(value.action)){
                    //匹配后输出key和它对应的pattern里的模型
                    out.collect(new Tuple2<>(ctx.getCurrentKey(),pattern));
                }
            }
            //更新状态
            prevActionState.update(value.action);
        }

        @Override
        public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            //从上下文中获取广播状态，并用当前数据更新状态
            BroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<Void, Pattern>("pattern", Types.VOID, Types.POJO(Pattern.class)));

            //更新当前的广播状态
            patternState.put(null,value);
        }
    }


    //定义用户行为事件和模式的POJO类
    public static class Action{
        public String userId; //id
        public String action; //行为

        public Action() {}

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action[" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    ']';
        }
    }

    public static class Pattern{
        public String action1; //行为 1
        public String action2; //行为 2

        public Pattern() {}

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern[" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    ']';
        }
    }
}

