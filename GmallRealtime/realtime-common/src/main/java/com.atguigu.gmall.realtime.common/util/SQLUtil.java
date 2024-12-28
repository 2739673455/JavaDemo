package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

public class SQLUtil {
    public static String getKafkaDDL(String topic, String groupId) {
        return " with ( " +
                "'connector' = 'kafka', " +
                "'topic' = '" + topic + "', " +
                "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                "'properties.group.id' = '" + groupId + "', " +
                "'scan.startup.mode' = 'latest-offset', " +
                "'format' = 'json' " +
                ") ";
    }

    public static String getKafkaDDL(String topic, String groupId, String startUpMode) {
        return " with ( " +
                "'connector' = 'kafka', " +
                "'topic' = '" + topic + "', " +
                "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                "'properties.group.id' = '" + groupId + "', " +
                "'scan.startup.mode' = '" + startUpMode + "', " +
                "'format' = 'json' " +
                ") ";
    }

    public static String getHBaseDDL(String tableName) {
        return " with ( " +
                "'connector' = 'hbase-2.2', " +
                "'table-name' = '" + Constant.HBASE_NAMESPACE + ":" + tableName + "', " +
                "'zookeeper.quorum' = 'hadoop102:2181,hadoop103:2181,hadoop104:2181', " +
                "'lookup.async' = 'true', " +
                "'lookup.cache' = 'PARTIAL', " +
                "'lookup.partial-cache.max-rows' = '500', " +
                "'lookup.partial-cache.expire-after-write' = '1 day', " +
                "'lookup.partial-cache.expire-after-access' = '1 day' " +
                ") ";
    }

    public static String getUpsertKafkaDDL(String topic) {
        return " with ( " +
                "'connector' = 'upsert-kafka', " +
                "'properties.bootstrap.servers'='" + Constant.KAFKA_BROKERS + "', " +
                "'topic'='" + topic + "', " +
                "'key.format'='json', " +
                "'value.format'='json' " +
                ") ";
    }

    public static String getDorisDDL(String tableName) {
        return "with ( " +
                "'connector' = 'doris', " +
                "'fenodes' = '" + Constant.DORIS_FE_NODES + "', " +
                "'table.identifier' = 'gmall." + tableName + "', " +
                "'username' = 'root', " +
                "'password' = 'aaaaaa', " +
                "'sink.properties.format' = 'json', " +
                "'sink.enable-2pc' = 'false' " +
                ") ";
    }
}
