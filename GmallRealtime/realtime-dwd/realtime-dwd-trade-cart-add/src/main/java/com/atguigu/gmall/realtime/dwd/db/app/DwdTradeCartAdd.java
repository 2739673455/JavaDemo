package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {

        // 从topic_db读取数据
        readOdsDb(streamTableEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);

        // 过滤加购数据
        Table cartAddInfo = streamTableEnv.sqlQuery(
                "select " +
                        "`data` ['id']                                                                                                               as id, " +
                        "`data` ['user_id']                                                                                                          as user_id, " +
                        "`data` ['sku_id']                                                                                                           as sku_id, " +
                        "if(`type` = 'insert', `data` ['sku_num'], cast((cast(`data` ['sku_num'] as int) - cast(`old` ['sku_num'] as int)) as string)) as sku_num, " +
                        "ts " +
                        "from " +
                        "topic_db " +
                        "where " +
                        "`table` = 'cart_info' " +
                        "and ( " +
                        "`type` = 'insert' " +
                        "or ( " +
                        "`type` = 'update' " +
                        "and `old` ['sku_num'] is not null " +
                        "and (cast(`data` ['sku_num'] as int) > cast(`old` ['sku_num'] as int)) " +
                        ") " +
                        ")"
        );

        // 将数据写入Kafka
        streamTableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + " ( " +
                        "id string, " +
                        "user_id string, " +
                        "sku_id string, " +
                        "sku_num string, " +
                        "ts bigint, " +
                        "primary key (id) not enforced " +
                        ") " +
                        SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD)
        );
        cartAddInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
