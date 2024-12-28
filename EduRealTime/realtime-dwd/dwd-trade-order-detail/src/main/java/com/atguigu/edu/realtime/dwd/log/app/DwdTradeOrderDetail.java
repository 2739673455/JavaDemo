package com.atguigu.edu.realtime.dwd.log.app;

import com.atguigu.edu.common.base.BaseSQLApp;
import com.atguigu.edu.common.constant.Constant;
import com.atguigu.edu.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
/*
 * 下单事务事实表
 * */


public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10041,4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment strTableEnv) {
        // 1.设置状态的失效时间  传输的延迟 + 业务上的滞后关系
        strTableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        // 2.从kafka的topic_db主题中读取数据
        readOdsDb(strTableEnv,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        // 3.从kafka读取日志数据，封装为Flink Sql 表
        strTableEnv.executeSql("create table page_log(" +
                "`common` map<String, String>,\n" +
                "`page` map<String, String>,\n" +
                "`ts` String\n" +
                ")" + SQLUtil.getKafkaSource("dwd_traffic_page", "edu1"));
//        strTableEnv.executeSql("select * from page_log").print();
        // 3.过滤出订单明细数据
        Table orderDetail = strTableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['user_id'] user_id,\n" +
                "data['course_id'] course_id,\n" +
                "date_format(data['create_time'], 'yyyy-MM-dd') date_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['origin_amount'] origin_amount,\n" +
                "data['coupon_reduce'] coupon_reduce,\n" +
                "data['final_amount'] final_amount,\n" +
                "ts\n" +
                "from `topic_db` where `table` = 'order_detail' " +
                "and `type` = 'insert' \n");
        strTableEnv.createTemporaryView("order_detail", orderDetail);
//        orderDetail.execute().print();

        // 4.过滤出订单数据
        Table orderInfo = strTableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['province_id'] province_id,\n" +
                "data['out_trade_no'] out_trade_no,\n" +
                "data['session_id'] session_id,\n" +
                "data['trade_body'] trade_body\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'insert'");
        strTableEnv.createTemporaryView("order_info", orderInfo);
//        orderInfo.execute().print();
        // 5.筛选订单页面日志，获取 source_id
        Table filteredLog =strTableEnv.sqlQuery("select " +
                "common['sid'] session_id,\n" +
                "common['sc'] source_id\n" +
                "from page_log\n" +
                "where page['page_id'] = 'order'");
        strTableEnv.createTemporaryView("filter_log", filteredLog);
        // 6.关联三张表获得订单明细表
        Table resultTable = strTableEnv.sqlQuery("select \n" +
                "od.id,\n" +
                "order_id,\n" +
                "user_id,\n" +
                "course_id,\n" +
                "province_id,\n" +
                "date_id,\n" +
                "oi.session_id,\n" +
                "source_id,\n" +
                "create_time,\n" +
                "origin_amount,\n" +
                "coupon_reduce coupon_reduce_amount,\n" +
                "final_amount,\n" +
                "out_trade_no,\n" +
                "trade_body,\n" +
                "ts \n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join filter_log fl\n" +
                "on oi.session_id = fl.session_id");
//        resultTable.execute().print();
        // 7.将关联的结果写到kafka主题
        strTableEnv.executeSql(
                "create table dwd_trade_order_detail(\n" +
                        "id string,\n" +
                        "order_id string,\n" +
                        "user_id string,\n" +
                        "course_id string,\n" +
                        "province_id string,\n" +
                        "date_id string,\n" +
                        "session_id string,\n" +
                        "source_id string,\n" +
                        "create_time string,\n" +
                        "origin_amount string,\n" +
                        "coupon_reduce_amount string,\n" +
                        "final_amount string,\n" +
                        "out_trade_no string,\n" +
                        "trade_body string,\n" +
                        "ts BIGINT,\n" +
                        "primary key(id) not enforced\n" +
                        ")" + SQLUtil.getUpsertKafkaSink(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        resultTable.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

    }
}
