package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.edu.common.base.BaseSQLApp;
import com.atguigu.edu.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 支付成功事实表
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10032, 4, "dwd_trade_order_payment_success");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {
        //TODO 0.设置状态失效时间
        streamTableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30));
        //todo 1.从Kafka的 topic_db中读取数据
        readOdsDb(streamTableEnv, "dwd_trade_order_payment_success");
        //todo 2.过滤出支付成功的行为 1602为支付成功的行为
        Table paymentInfo = streamTableEnv.sqlQuery(
                "select\n" +
                        "    `data`['id'] id,\n" +
                        "    `data`['out_trade_no'] out_trade_no,\n" +
                        "    `data`['total_amount'] total_amount,\n" +
                        "    `data`['create_time'] create_time,\n" +
                        "    `ts`\n" +
                        "from topic_db\n" +
                        "where `table` = 'payment_info'\n" +
                        "and `data`['payment_status'] = '1602'"
        );
        streamTableEnv.createTemporaryView("paymentInfo", paymentInfo);
        //todo 3.过滤出订单表的数据
        Table orderInfo = streamTableEnv.sqlQuery(
                "select\n" +
                        "    `data`['user_id'] user_id,\n" +
                        "    `data`['out_trade_no'] out_trade_no,\n" +
                        "    `ts`\n" +
                        "from topic_db\n" +
                        "where `table` = 'order_info'"
        );
        streamTableEnv.createTemporaryView("orderInfo", orderInfo);
        //TODO 4.进行Join取到支付成功人的user_id
        Table result = streamTableEnv.sqlQuery(
                "select\n" +
                        "    t1.id,\n" +
                        "    t2.user_id,\n" +
                        "    t1.total_amount,\n" +
                        "    t1.create_time,\n" +
                        "    t1.ts\n" +
                        "from (\n" +
                        "    select\n" +
                        "        id,\n" +
                        "        out_trade_no,\n" +
                        "        total_amount,\n" +
                        "        create_time,\n" +
                        "        ts\n" +
                        "    from paymentInfo\n" +
                        "     ) t1\n" +
                        "join (\n" +
                        "    select\n" +
                        "        user_id,\n" +
                        "        out_trade_no\n" +
                        "    from orderInfo\n" +
                        "     )t2\n" +
                        "on t1.out_trade_no = t2.out_trade_no;"
        );
        streamTableEnv.createTemporaryView("result", result);
        //TODO 5.将关联的结果写到Kafka中
        streamTableEnv.executeSql(
                "create table dwd_trade_order_payment_success(\n" +
                        "    id string,\n" +
                        "    user_id string,\n" +
                        "    total_amount string,\n" +
                        "    create_time string,\n" +
                        "    ts bigint,\n" +
                        "    primary key (`id`) not enforced\n" +
                        ")" + SQLUtil.getUpsertKafkaSink("dwd_trade_order_payment_success")
        );

        result.executeInsert("dwd_trade_order_payment_success");

    }
}
