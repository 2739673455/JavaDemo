package com.atguigu.edu.realtime.dws.trade.source.order.app;

import com.atguigu.edu.common.base.BaseSQLApp;
import com.atguigu.edu.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Result extends BaseSQLApp {
    public static void main(String[] args) {
        new Result().start(
                10023,
                4,
                "rest"
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {
        streamTableEnv.executeSql("CREATE TABLE result_detail(\n" +
                "     id string,\n" +
                "     order_id string,\n" +
                "     user_id string,\n" +
                "     source_id string,\n" +
                "     source_name string,\n" +
                "     origin_amount string,\n" +
                "     coupon_reduce string,\n" +
                "     final_amount string,\n" +
                "     ts bigint,\n" +
                "     et as TO_TIMESTAMP_LTZ(ts,0),\n" +
                "     WATERMARK FOR et AS et\n" +
                ")"+ SQLUtil.getKafkaSource("result","rest"));


//          streamTableEnv.executeSql("select * from result_detail").print();



        Table resTable = streamTableEnv.sqlQuery("select\n" +
                "   DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "   DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "   DATE_FORMAT(window_start, 'yyyy-MM-dd') cur_date,\n" +
                 "   source_id,\n" +
                 "   source_name,\n" +
                "   sum(cast(final_amount as decimal(16,2))) final_amount,\n" +
                "   count(distinct order_id) order_count,\n" +
                "   count(distinct user_id) user_count\n" +
                "FROM TABLE(\n" +
                "  TUMBLE(TABLE result_detail,DESCRIPTOR(`et`),INTERVAL '30' second))\n" +
                "GROUP BY source_id,source_name,window_start,window_end");
//         resTable.execute().print();
        streamTableEnv.executeSql("CREATE TABLE dws_trade_source_order_window(\n" +
                "    stt string,\n" +
                "    edt string,\n" +
                "    cur_date string,\n" +
                "    source_id string,\n" +
                "    source_name string,\n" +
                "    final_amount decimal,\n" +
                "    order_count bigint,\n" +
                "    user_count bigint\n" +
                ")"+SQLUtil.getDorisSink("dws_trade_source_order_window"));
        resTable.executeInsert("dws_trade_source_order_window");


    }
}
