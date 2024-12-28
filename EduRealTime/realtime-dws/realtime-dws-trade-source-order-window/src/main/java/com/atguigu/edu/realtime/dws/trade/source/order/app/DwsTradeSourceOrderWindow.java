package com.atguigu.edu.realtime.dws.trade.source.order.app;

import com.atguigu.edu.common.base.BaseSQLApp;
import com.atguigu.edu.common.constant.Constant;
import com.atguigu.edu.common.util.SQLUtil;
import com.atguigu.edu.realtime.dws.trade.source.order.function.SourceJoin;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwsTradeSourceOrderWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTradeSourceOrderWindow().start(
                10022,
                4,
                "dws_trade_source_order_window"
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {

        streamTableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        streamTableEnv.executeSql("CREATE TABLE order_detail(\n" +
                "     id string,\n" +
                "     order_id string,\n" +
                "     user_id string,\n" +
                "     origin_amount string,\n" +
                "     coupon_reduce string,\n" +
                "     final_amount string,\n" +
                "     ts bigint\n," +
                "     pt as proctime() "+
                ")"+ SQLUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,"dws_trade_source_order"));
//         streamTableEnv.executeSql("select * from order_detail").print();
//         streamTableEnv.sqlQuery("select * from order_detail limit 50").printSchema();


        streamTableEnv.executeSql("CREATE TABLE dwd_traffic_page(\n" +
                " common map<string,string>\n" +
                ")"+SQLUtil.getKafkaSource(Constant.TOPIC_DWD_TRAFFIC_PAGE, "dws_trade_source_order_window"));


        Table sourceTable = streamTableEnv.sqlQuery("select\n" +
                "   common['uid'] uid,\n" +
                "   common['sc'] sc\n" +
                "from dwd_traffic_page");
        streamTableEnv.createTemporaryView("dwd_traffic_page",sourceTable);
//         sourceTable.execute().print();

        SourceJoin.readBaseSource(streamTableEnv);




        Table withSourceTable = streamTableEnv.sqlQuery("select\n" +
                "    od.id,\n" +
                "    od.order_id,\n" +
                "    od.user_id,\n" +
                "    bs.id source_id,\n" +
                "    bs.source_site source_name,\n" +
                "    od.origin_amount,\n" +
                "    od.coupon_reduce,\n" +
                "    od.final_amount,\n" +
                "    od.ts\n" +
                "from  order_detail od \n" +
                "  join dwd_traffic_page tp \n" +
                 " on od.user_id=tp.uid \n" +
                "  left join base_source FOR SYSTEM_TIME AS OF od.pt AS bs " +
                "  on tp.sc=bs.id");



//         withSourceTable.execute().print();
        // tableEnv.createTemporaryView("with_source_order",withSourceTable);

         streamTableEnv.executeSql("CREATE TABLE kafka_detail(\n" +
                         "     id string,\n" +
                         "     order_id string,\n" +
                         "     user_id string,\n" +
                         "     source_id string,\n" +
                         "     source_name string,\n" +
                         "     origin_amount string,\n" +
                         "     coupon_reduce string,\n" +
                         "     final_amount string,\n" +
                         "     ts bigint,\n" +
                         "     PRIMARY KEY (id) NOT ENFORCED\n" +
                         ")"+ SQLUtil.getUpsertKafkaSink("result"));
        withSourceTable.executeInsert("kafka_detail");


    }
}
