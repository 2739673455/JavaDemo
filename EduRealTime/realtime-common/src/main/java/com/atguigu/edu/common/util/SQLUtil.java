package com.atguigu.edu.common.util;


import com.atguigu.edu.common.constant.Constant;

public class SQLUtil {
    public static String getKafkaSource(String topic, String groupId) {
        return " with ( " +
                "'connector' = 'kafka', " +
                "'topic' = '" + topic + "', " +
                "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                "'properties.group.id' = '" + groupId + "', " +
                "'scan.startup.mode' = 'latest-offset', " +
                "'format' = 'json' " +
                ") ";
    }

    public static String getKafkaSource(String topic, String groupId, String startUpMode) {
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
                "'zookeeper.quorum' = '" + Constant.HBASE_ZK_QUORUM + "', " +
                "'lookup.async' = 'true', " +
                "'lookup.cache' = 'PARTIAL', " +
                "'lookup.partial-cache.max-rows' = '500', " +
                "'lookup.partial-cache.expire-after-write' = '1 day', " +
                "'lookup.partial-cache.expire-after-access' = '1 day' " +
                ") ";
    }

    public static String getUpsertKafkaSink(String topic) {
        return " with ( " +
                "'connector' = 'upsert-kafka', " +
                "'properties.bootstrap.servers'='" + Constant.KAFKA_BROKERS + "', " +
                "'topic'='" + topic + "', " +
                "'key.format'='json', " +
                "'value.format'='json' " +
                ") ";
    }

    public static String getDorisSink(String tableName) {
        return "with ( " +
                "'connector' = 'jdbc', " +
                "'driver'='" + Constant.MYSQL_DRIVER + "', " +
                "'url' = '" + Constant.DORIS_JDBC_URL + "', " +
                "'table-name' = '" + tableName + "', " +
                "'username'='" + Constant.DORIS_USER_NAME + "', " +
                "'password'='" + Constant.DORIS_PASSWORD + "' " +
                ") ";
    }

    //public static String getDorisDDL(String tableName) {
    //    return "with ( " +
    //            "'connector' = 'doris', " +
    //            "'fenodes' = '" + Constant.DORIS_FE_NODES + "', " +
    //            "'table.identifier' = '" + Constant.DORIS_DATABASE + "." + tableName + "', " +
    //            "'username' = '" + Constant.DORIS_USER + "', " +
    //            "'password' = '" + Constant.DORIS_PASSWD + "', " +
    //            "'sink.properties.format' = 'json', " +
    //            "'sink.enable-2pc' = 'false' " +
    //            ") ";
    //}
}
