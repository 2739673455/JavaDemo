package com.atguigu.edu.common.constant;

public class Constant {
    // Kafka
    public static final String KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    // Mysql
    public static final String MYSQL_HOST = "hadoop100";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "000000";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop100:3306?useSSL=false";

    // HBase
    public static final String HBASE_ZK_QUORUM = "hadoop102:2181,hadoop103:2182,hadoop104:2183";
    public static final String HBASE_NAMESPACE = "edu";

    // Redis
    public static final String REDIS_HOST = "hadoop101";
    public static final int REDIS_PORT = 6379;
    public static final String REDIS_URL = "redis://hadoop101:6379/0";

    // Doris
    public static final String DORIS_JDBC_URL = "jdbc:mysql://hadoop100:9030/edu";
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
