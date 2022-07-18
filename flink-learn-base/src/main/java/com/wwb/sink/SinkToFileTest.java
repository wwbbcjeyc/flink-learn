package com.wwb.sink;

import com.wwb.bean.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 21:54
 * @Version 1.0
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Alice", "./prod?id=200", 3200L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );


        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy( // 滚动策略
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024) //文件大小
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) // 时间间隔
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) //隔五分钟数据没有来,开始准备新的文件
                                .build()
                )
                .build();

        //将数据格式化之后打印输出到文件上
        stream.map(data -> data.toString()).addSink(streamingFileSink);

        env.execute();
    }
}

