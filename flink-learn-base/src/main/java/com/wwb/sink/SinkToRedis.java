package com.wwb.sink;

import com.wwb.bean.Event;
import com.wwb.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author wangwenbo
 * @Date 2022/5/2 21:58
 * @Version 1.0
 */
public class SinkToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //自定义测试数据源
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        //创建一个jedis的连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        //写入redis
        stream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute();
    }

    //自定义类实现RedisMapper接口
    public static class MyRedisMapper implements RedisMapper<Event> {

        //返回一个redis操作命令的描述
        @Override
        public RedisCommandDescription getCommandDescription() {
            // 保存在Hset表中,表名click
            return new RedisCommandDescription(RedisCommand.HSET, "click");
        }

        //返回当前的key
        @Override
        public String getKeyFromData(Event data) {
            return data.user;
        }

        //返回当前的value
        @Override
        public String getValueFromData(Event data) {
            return data.url;
        }
    }
}

