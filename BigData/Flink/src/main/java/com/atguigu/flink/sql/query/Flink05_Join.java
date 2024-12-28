package com.atguigu.flink.sql.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink05_Join {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);


        // 常规Join: 使用常规Join时，会将两条流的数据维护到状态中，一般使用TTL
        //SingleOutputStreamOperator<OrderEvent> orderDs = env.socketTextStream("localhost", 9999)
        //        .map(line -> new OrderEvent(Long.parseLong(line.split(",")[0]), Long.parseLong(line.split(",")[1])))
        //        .returns(Types.POJO(OrderEvent.class));
        //
        //SingleOutputStreamOperator<OrderDetailEvent> orderDetailDs = env.socketTextStream("localhost", 9990)
        //        .map(line -> new OrderDetailEvent(Long.parseLong(line.split(",")[0]), Long.parseLong(line.split(",")[1])))
        //        .returns(Types.POJO(OrderDetailEvent.class));
        //
        //streamTableEnv.getConfig().getConfiguration().setLong("table.exec.state.ttl", 3 * 1000); // 设置TTL
        //
        //Schema orderSchema = Schema.newBuilder()
        //        .column("id", "bigint")
        //        .column("ts", "bigint")
        //        .columnByExpression("process_time", "proctime()")
        //        .columnByExpression("event_time", "to_timestamp_ltz(ts,3)")
        //        .watermark("event_time", "event_time - interval '0' second")
        //        .build();
        //Table orderTable = streamTableEnv.fromDataStream(orderDs, orderSchema);
        //streamTableEnv.createTemporaryView("order_t", orderTable);
        //
        //Schema orderDetailSchema = Schema.newBuilder()
        //        .column("id", "bigint")
        //        .column("ts", "bigint")
        //        .columnByExpression("process_time", "proctime()")
        //        .columnByExpression("event_time", "to_timestamp_ltz(ts,3)")
        //        .watermark("event_time", "event_time - interval '0' second")
        //        .build();
        //Table orderDetailTable = streamTableEnv.fromDataStream(orderDetailDs, orderDetailSchema);
        //streamTableEnv.createTemporaryView("order_detail_t", orderDetailTable);
        //
        //streamTableEnv.sqlQuery(
        //        "select order_t.id, order_t.ts, odd.id, odd.ts " +
        //                "from order_t join order_detail_t as odd on order_t.id=odd.id"
        //).execute().print();

        // 间隔Join
        //streamTableEnv.sqlQuery(
        //        "select order_t.id, order_t.ts, order_detail_t.id, order_detail_t.ts " +
        //                "from order_t, order_detail_t " +
        //                "where order_t.id=order_detail_t.id " +
        //                "and order_detail_t.event_time between order_t.event_time-interval '2' second and order_t.event_time+interval '2' second"
        //).execute().print();

        // Lookup Join 维表Join
        SingleOutputStreamOperator<OrderDetailSKUEvent> orderDetailSKUDs = env.socketTextStream("localhost", 9980)
                .map(line -> new OrderDetailSKUEvent(Long.parseLong(line.split(",")[0]), Long.parseLong(line.split(",")[1]), Long.parseLong(line.split(",")[2])))
                .returns(Types.POJO(OrderDetailSKUEvent.class));

        Schema orderDetailSKUSchema = Schema.newBuilder()
                .column("id", "bigint")
                .column("skuId", "bigint")
                .column("ts", "bigint")
                .columnByExpression("process_time", "proctime()")
                .columnByExpression("event_time", "to_timestamp_ltz(ts,3)")
                .watermark("event_time", "event_time - interval '0' second")
                .build();
        Table orderDetailSKUTable = streamTableEnv.fromDataStream(orderDetailSKUDs, orderDetailSKUSchema);
        streamTableEnv.createTemporaryView("order_detail_sku_t", orderDetailSKUTable);

        streamTableEnv.executeSql(
                "create table sku_info(id bigint, sku_name string) " +
                        "with( " +
                        "'connector'='jdbc', " +
                        "'driver'='com.mysql.cj.jdbc.Driver', " +
                        "'url'='jdbc:mysql://localhost:3306/db1', " +
                        "'table-name'='sku_info', " +
                        "'username'='root', " +
                        "'password'='123321', " +
                        "'lookup.cache'='partial', " +
                        "'lookup.partial-cache.max-rows'='1000', " +
                        "'lookup.partial-cache.expire-after-write'='20 second', " +
                        "'lookup.partial-cache.expire-after-access'='20 second' " +
                        ")"
        );

        streamTableEnv.sqlQuery(
                "select odds.id, odds.skuId, odds.ts, sku_info.id, sku_info.sku_name " +
                        "from order_detail_sku_t as odds " +
                        "join sku_info " +
                        "for system_time as of odds.process_time " +
                        "on odds.skuId=sku_info.id "
        ).execute().print();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderEvent {
        private Long id;
        private Long ts;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderDetailEvent {
        private Long id;
        private Long ts;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderDetailSKUEvent {
        private Long id;
        private Long skuId;
        private Long ts;
    }
}
