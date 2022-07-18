package com.wwb.async;

import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * @Author wangwenbo
 * @Date 2022/5/3 10:54
 * @Version 1.0
 */
public class AsyncDataRequestDemo {

    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<JSONObject> sensorDS = env
                .socketTextStream("127.0.0.1", 9999)
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {

                        JSONObject jsonObject = JSONObject.parseObject(value);


                        return jsonObject;

                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                Long event_time = element.getLong("event_time");
                                return event_time * 1000L;
                            }
                        }));

        // sensorDS.print();

        SingleOutputStreamOperator<JSONObject> stringSingleOutputStreamOperator = AsyncDataStream.unorderedWait(sensorDS,
                new MyRichAsyFunction(), 5000, TimeUnit.MILLISECONDS, 40).setParallelism(1);

        stringSingleOutputStreamOperator.print("result:");
        env.execute();
    }

    private static class MyRichAsyFunction extends RichAsyncFunction<JSONObject, JSONObject> {

        private RedisClient redisClient;
        private StatefulRedisConnection<String, String> connect;
        private RedisAsyncCommands<String, String> asyncCommand;
        private RedisCommands<String, String> syncCommand;

        private transient volatile Cache<String, String> increaseMap;


        @Override
        public void open(Configuration parameters) throws Exception {


            redisClient = RedisClient.create(RedisURI.builder()
                    .withHost("127.0.0.1")
                    .withPort(6379)
                    .build());
            connect = redisClient.connect();
            asyncCommand = connect.async();
            syncCommand = connect.sync();

            //缓存设置
            increaseMap = CacheBuilder.<Tuple2<String, String>, String>newBuilder().
                    maximumSize(10).
                    expireAfterWrite(5, TimeUnit.MINUTES)
                    .build();

            super.open(parameters);
        }

        @Override
        public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) {

            String tuid = input.getString("tuid");


            String ifPresent = increaseMap.getIfPresent(tuid);

            System.out.println("111");
            if (StringUtils.isNotEmpty(ifPresent)) {

                input.put("flage","1");
                System.out.println("222");
                resultFuture.complete(Collections.singleton(input));
            }else {
                RedisFuture<Long> exists = asyncCommand.exists(tuid);

                System.out.println("333");
                CompletableFuture.supplyAsync(new Supplier<Long>() {
                    @Override
                    public Long get() {
                        try {
                            Long s = exists.get();
                            return s;
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                            return null;
                        }
                    }
                }).thenAccept((dbResult) ->{
                    System.out.println("查询结果"+dbResult);
                    if(dbResult >0){
                        increaseMap.put(tuid, String.valueOf(System.currentTimeMillis() / 1000));
                        input.put("flage","1");
                        System.out.println("444");

                    }else {
                        System.out.println("555");
                        input.put("flage","0");
                    }
                    System.out.println("缓存是否存在:"+increaseMap.getIfPresent(tuid));
                    resultFuture.complete(Collections.singleton(input));
                });
            }

        }

        @Override
        public void close() throws Exception {
            super.close();
            if (connect != null) {
                connect.close();
                redisClient.shutdown();
            }
        }

       /* @Override
        public void timeout(WaterSensor input, ResultFuture<String> resultFuture) {
            //超时了。丢弃即可
            System.out.println("超时了："+input);
            resultFuture.complete(Collections.singleton("0"));
           *//* resultFuture.completeExceptionally(
                    new TimeoutException("Async function call has timed out."));*//*
           // System.out.println("进入超时方法");
          //  asyncInvoke(input,resultFuture);
        }*/
    }
}

