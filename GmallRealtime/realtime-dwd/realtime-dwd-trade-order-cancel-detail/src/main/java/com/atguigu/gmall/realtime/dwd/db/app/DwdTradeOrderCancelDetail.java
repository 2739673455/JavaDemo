package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderCancelDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(10015, 4, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {

        // 读取Kafka topic_db数据
        readOdsDb(streamTableEnv, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
        streamTableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 5));

        // 过滤出取消订单数据
        Table orderCancel = streamTableEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['operate_time'] operate_time, " +
                        " `ts` " +
                        "from topic_db " +
                        "where `table`='order_info' " +
                        "and `type`='update' " +
                        "and `old`['order_status']='1001' " +
                        "and `data`['order_status']='1003' ");
        streamTableEnv.createTemporaryView("order_cancel", orderCancel);

        // 从Kafka读取 dwd_trade_order_detail 数据
        streamTableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " ( " +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint " +
                        ")" +
                        SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL)
        );

        // 进行关联
        Table result = streamTableEnv.sqlQuery(
                "select  " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id "
        );

        // 输出到Kafka
        streamTableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + " ( " +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "cancel_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint, " +
                        "primary key (id) not enforced " +
                        ")" +
                        SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL)
        );
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
}
