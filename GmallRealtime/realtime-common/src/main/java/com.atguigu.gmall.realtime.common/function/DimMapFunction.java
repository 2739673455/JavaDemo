package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

// 使用旁路缓存进行维度关联
public abstract class DimMapFunction<T> extends RichMapFunction<T, T> {
    Connection hbaseConnection;
    Jedis jedis;
    JSONObject dimJson = null;

    @Override
    public void open(Configuration parameters) {
        hbaseConnection = HBaseUtil.getHbaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() {
        HBaseUtil.closeHBaseConnection(hbaseConnection);
        RedisUtil.close(jedis);
    }

    @Override
    public T map(T in) throws Exception {
        String tableName = getTableName(in);
        String rowKey = getRowKey(in);
        dimJson = RedisUtil.readDim(jedis, tableName, rowKey);
        if (dimJson == null) {
            dimJson = HBaseUtil.getRow(hbaseConnection, Constant.HBASE_NAMESPACE, tableName, rowKey, JSONObject.class);
            if (dimJson != null) {
                RedisUtil.writeDim(jedis, tableName, rowKey, dimJson);
            } else {
                System.out.println("HBase中 " + tableName + ":" + rowKey + " 维度数据不存在");
            }
        }
        if (dimJson != null) {
            addDim(in, dimJson);
        }
        return in;
    }

    public abstract String getRowKey(T in);

    public abstract String getTableName(T in);

    public abstract void addDim(T in, JSONObject dimJson);
}
