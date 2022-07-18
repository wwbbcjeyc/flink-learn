/*
package com.zjtd.dp.async;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

*/
/**
 * @Author wangwenbo
 * @Date 2021/7/31 11:03 上午
 * @Version 1.0
 *//*

class AsyncDatabaseRequest extends RichAsyncFunction<String, Tuple2<String, String>> {

    */
/** 能够利用回调函数并发发送请求的数据库客户端 *//*

    private transient DatabaseClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        client = new DatabaseClient(host, post, credentials);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void asyncInvoke(String key, final ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {

        // 发送异步请求，接收 future 结果
        final Future<String> result = client.query(key);

        // 设置客户端完成请求后要执行的回调函数
        // 回调函数只是简单地把结果发给 future
        CompletableFuture.supplyAsync(new Supplier<String>() {

            @Override
            public String get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // 显示地处理异常。
                    return null;
                }
            }
        }).thenAccept( (String dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(key, dbResult)));
        });
    }
}

    // 创建初始 DataStream
    DataStream<String> stream = ...;

// 应用异步 I/O 转换操作
        DataStream<Tuple2<String, String>> resultStream =
        AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100);*/
