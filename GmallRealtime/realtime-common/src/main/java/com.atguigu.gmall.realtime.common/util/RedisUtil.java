package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    private static JedisPool jedisPool;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxIdle(10);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379, 10000);
    }

    public static Jedis getJedis() {
        return jedisPool.getResource();
    }

    public static void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        String key = tableName + ":" + id;
        String dimStr = jedis.get(key);
        if (dimStr != null && !dimStr.isEmpty()) {
            return JSONObject.parseObject(dimStr);
        }
        return null;
    }

    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dim) {
        String key = tableName + ":" + id;
        jedis.setex(key, 86400, dim.toJSONString());
    }

    // 获取异步操作Redis客户端的连接对象
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/0");
        return redisClient.connect();
    }

    // 关闭Redis异步客户端连接
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> connect) {
        if (connect != null) {
            connect.close();
        }
    }

    // 异步从Redis读数据
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> connect, String tableName, String id) {
        String key = tableName + ":" + id;
        RedisAsyncCommands<String, String> commands = connect.async();
        try {
            String dimStr = commands.get(key).get();
            if (dimStr != null && !dimStr.isEmpty()) {
                return JSONObject.parseObject(dimStr);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    // 异步向Redis写数据
    public static void writeDimAsync(StatefulRedisConnection<String, String> connect, String tableName, String id, JSONObject dim) {
        String key = tableName + ":" + id;
        RedisAsyncCommands<String, String> commands = connect.async();
        commands.setex(key, 86400, dim.toJSONString());
    }
}
