package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeRefundPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(10018, 4, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {

        // 从Kafka topic_db读取数据
        readOdsDb(streamTableEnv, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);

        // 从HBase读取 base_dic 数据
        readDimDic(streamTableEnv);

        // 过滤退款成功表数据
        Table refundPayment = streamTableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['payment_type'] payment_type," +
                        "data['callback_time'] callback_time," +
                        "data['total_amount'] total_amount," +
                        "process_time, " +
                        "ts " +
                        "from topic_db " +
                        "where `table`='refund_payment' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status']='1602'");
        streamTableEnv.createTemporaryView("refund_payment", refundPayment);

        // 过滤退单表中的退单成功的数据
        Table orderRefundInfo = streamTableEnv.sqlQuery(
                "select " +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_num'] refund_num " +
                        "from topic_db " +
                        "where `table`='order_refund_info' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status']='0705'");
        streamTableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 过滤订单表中的退款成功的数据
        Table orderInfo = streamTableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['province_id'] province_id " +
                        "from topic_db " +
                        "where `table`='order_info' " +
                        "and `type`='update' " +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status']='1006'");
        streamTableEnv.createTemporaryView("order_info", orderInfo);

        // 进行关联
        Table result = streamTableEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.process_time as dic " +
                        "on rp.payment_type=dic.dic_code ");

        // 写入Kafka
        streamTableEnv.executeSql("create table dwd_trade_refund_payment_success (" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint, " +
                "primary key (id) not enforced " +
                ")" +
                SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS)
        );
        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }
}
