package com.atguigu.edu.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.constant.Constant;
import com.atguigu.edu.common.util.HBaseUtil;
import com.atguigu.edu.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

// 使用异步+旁路缓存进行维度关联
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection hBaseAsyncConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConnection = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConnection);
        HBaseUtil.closeHBaseAsyncConnection(hBaseAsyncConnection);
    }

    @Override
    public void asyncInvoke(T in, ResultFuture<T> resultFuture) {
        String tableName = getTableName();
        String rowKey = getRowKey(in);
        // 创建异步编排对象，返回值作为下一个线程任务的参数
        CompletableFuture
                .supplyAsync(
                        new Supplier<JSONObject>() {
                            @Override
                            public JSONObject get() {
                                return RedisUtil.readDimAsync(redisAsyncConnection, tableName, rowKey);
                            }
                        }
                )
                // 有入参，有返回值的线程任务
                .thenApplyAsync(
                        new Function<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject apply(JSONObject dimJson) {
                                if (dimJson == null) {
                                    dimJson = HBaseUtil.readDimAsync(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, tableName, rowKey);
                                    if (dimJson != null) {
                                        RedisUtil.writeDimAsync(redisAsyncConnection, tableName, rowKey, dimJson);
                                    } else {
                                        System.out.println("HBase中 " + tableName + ":" + rowKey + " 维度数据不存在");
                                    }
                                }
                                return dimJson;
                            }
                        }
                )
                // 有入参，无返回值的线程任务
                .thenAcceptAsync(
                        new Consumer<JSONObject>() {
                            @Override
                            public void accept(JSONObject dimJson) {
                                if (dimJson != null)
                                    addDim(in, dimJson);
                                resultFuture.complete(Collections.singleton(in));
                            }
                        }
                );
    }

    public abstract String getRowKey(T in);

    public abstract String getTableName();

    public abstract void addDim(T in, JSONObject dimJson);

}
