package com.atguigu.edu.common.constant;

public class ConstantLocal {
    // Kafka
    public static final String KAFKA_BROKERS = "localhost:9092,localhost:9093,localhost:9094";

    // Mysql
    public static final String MYSQL_HOST = "localhost";
    public static final int MYSQL_PORT = 33068;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "000000";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://localhost:33068?useSSL=false";

    // HBase
    public static final String HBASE_ZK_QUORUM = "localhost:2181,localhost:2182,localhost:2183";
    public static final String HBASE_NAMESPACE = "edu";

    // Redis
    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;
    public static final String REDIS_URL = "redis://localhost:6379/0";

    // Doris
    public static final String DORIS_JDBC_URL = "jdbc:mysql://localhost:9030/edu";
    public static final String DORIS_USER_NAME = "root";
    public static final String DORIS_PASSWORD = "aaaaaa";

    // Kafka TopicConstant
    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_APPVIDEO = "dwd_traffic_appVideo";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

}
