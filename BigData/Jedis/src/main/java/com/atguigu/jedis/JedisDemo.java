package com.atguigu.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.params.SetParams;

public class JedisDemo {
    public static final String host = "hadoop102";
    public static final int port = 6379;

    //获取Jedis客户端对象
    public static Jedis getJedis() {
        //return new Jedis(host, port);
        return jedisPool.getResource();
    }

    //关闭Jedis
    public static void closeJedis(Jedis jedis) {
        if (jedis != null) jedis.close();
    }

    //连接池对象
    static JedisPool jedisPool;

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        config.setMaxIdle(5);
        config.setMinIdle(5);
        config.setBlockWhenExhausted(true);
        config.setMaxWaitMillis(2000);
        config.setTestOnBorrow(true);

        jedisPool = new JedisPool(config, host, port);
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();

        System.out.println(jedis.dbSize());
        System.out.println(jedis.keys("*"));

        testString(jedis);
        testList(jedis);
        testSet(jedis);
        testZSet(jedis);
        testHash(jedis);

        closeJedis(jedis);
    }

    public static void testString(Jedis jedis) {
        System.out.println(jedis.set("ping", "pong"));
        System.out.println(jedis.setnx("pong", "ping"));
        System.out.println(jedis.set("pong", "pong", SetParams.setParams().xx()));
        System.out.println(jedis.append("ping", "ping"));
        System.out.println(jedis.strlen("ping"));
        System.out.println(jedis.get("ping"));
        System.out.println(jedis.get("pong"));
        System.out.println(jedis.del("ping"));
        System.out.println(jedis.exists("ping"));
        System.out.println(jedis.exists("pong"));
        System.out.println(jedis.set("digit", "1"));
        System.out.println(jedis.get("digit"));
        System.out.println(jedis.incr("digit"));
        System.out.println(jedis.decr("digit"));
        System.out.println(jedis.incrBy("digit", 9));
        System.out.println(jedis.decrBy("digit", 9));
        System.out.println(jedis.mset("k1", "value1", "k2", "value2", "key3", "value3"));
        System.out.println(jedis.mget("k1", "k2", "k3"));
        System.out.println(jedis.setrange("pong", 0, "ppp"));
        System.out.println(jedis.getrange("pong", 0, 3));
        System.out.println(jedis.setex("k3", 3, "value3"));
        System.out.println(jedis.getSet("k3", "key3"));
    }

    public static void testList(Jedis jedis) {
        System.out.println(jedis.del("list1"));
        System.out.println(jedis.lpush("list1", "value1", "value2", "value3"));
        System.out.println(jedis.rpush("list1", "value4", "value5", "value6"));
        System.out.println(jedis.lpop("list1"));
        System.out.println(jedis.rpop("list1"));
        System.out.println(jedis.llen("list1"));
        System.out.println(jedis.rpoplpush("list1", "list1"));
        System.out.println(jedis.lrange("list1", 0, -1));
        System.out.println(jedis.lindex("list1", 0));
        System.out.println(jedis.linsert("list1", ListPosition.AFTER, "value1", "value5"));
        System.out.println(jedis.lrange("list1", 0, -1));
        System.out.println(jedis.lrem("list1", 2, "value5"));
        System.out.println(jedis.lrange("list1", 0, -1));
    }

    public static void testSet(Jedis jedis) {
        System.out.println(jedis.sadd("set1", "1", "2", "3", "4", "5", "6"));
        System.out.println(jedis.smembers("set1"));
        System.out.println(jedis.scard("set1"));
        System.out.println(jedis.sismember("set1", "1"));
        System.out.println(jedis.srem("set1", "1"));
        System.out.println(jedis.spop("set1"));
        System.out.println(jedis.srandmember("set1"));
        System.out.println(jedis.sadd("set2", "5", "4", "3", "2", "1"));
        System.out.println(jedis.sinter("set1", "set2"));
        System.out.println(jedis.sunion("set1", "set2"));
        System.out.println(jedis.sdiff("set1", "set2"));
        System.out.println(jedis.sdiff("set2", "set1"));
    }

    public static void testZSet(Jedis jedis) {
        System.out.println(jedis.zadd("zset1", 99, "r"));
        System.out.println(jedis.zadd("zset1", 98, "e"));
        System.out.println(jedis.zadd("zset1", 97, "d"));
        System.out.println(jedis.zadd("zset1", 96, "i"));
        System.out.println(jedis.zadd("zset1", 95, "s"));
        System.out.println(jedis.zrange("zset1", 0, -1));
        System.out.println(jedis.zrevrangeWithScores("zset1", 0, -1));
        System.out.println(jedis.zrangeByScore("zset1", 97, 99));
        System.out.println(jedis.zincrby("zset1", 1, "t"));
        System.out.println(jedis.zrevrangeByScore("zset1", 99, 0));
        System.out.println(jedis.zrem("zset1", "t"));
        System.out.println(jedis.zcount("zset1", 0, 100));
        System.out.println(jedis.zrank("zset1", "r"));
    }

    public static void testHash(Jedis jedis) {
        System.out.println(jedis.hset("user:1001", "id", "1001"));
        System.out.println(jedis.hset("user:1001", "name", "zhangsan"));
        System.out.println(jedis.hset("user:1001", "age", "88"));
        System.out.println(jedis.hsetnx("user:1001", "gender", "f"));
        System.out.println(jedis.hsetnx("user:1001", "gender", "f"));
        System.out.println(jedis.hget("user:1001", "gender"));
        System.out.println(jedis.hexists("user:1001", "gender"));
        System.out.println(jedis.hkeys("user:1001"));
        System.out.println(jedis.hvals("user:1001"));
        System.out.println(jedis.hincrBy("user:1001", "age", 10));
    }
}
