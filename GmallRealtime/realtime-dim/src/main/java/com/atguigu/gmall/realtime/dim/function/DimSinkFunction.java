package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

public class DimSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConnection;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) {
        hbaseConnection = HBaseUtil.getHbaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) {
        JSONObject jsonObject = value.f0;
        TableProcessDim tableProcessDim = value.f1;
        String type = jsonObject.getString("type");
        jsonObject.remove("type");
        // 根据业务数据库维度表进行的操作的类型对HBase进行操作
        String sinkTable = tableProcessDim.getSinkTable();
        String rowKey = jsonObject.getString(tableProcessDim.getSinkRowKey());
        if ("delete".equals(type)) {
            // 删除数据
            HBaseUtil.deleteRow(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
        } else {
            // 增改查，put
            HBaseUtil.putRow(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable, rowKey, tableProcessDim.getSinkFamily(), jsonObject);
        }
        // 维度数据变化后，清除redis中缓存
        jedis.del(sinkTable + ":" + rowKey);

    }

    @Override
    public void close() {
        HBaseUtil.closeHBaseConnection(hbaseConnection);
        RedisUtil.close(jedis);
    }
}
