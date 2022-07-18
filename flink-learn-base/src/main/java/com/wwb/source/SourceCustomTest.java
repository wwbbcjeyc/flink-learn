package com.wwb.source;


import com.wwb.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 21:51
 * @Version 1.0
 */
public class SourceCustomTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //自定义数据源
        DataStreamSource<Event> customSource = env.addSource(new ClickSource());

        customSource.print();

        env.execute("Custom");

    }
}
