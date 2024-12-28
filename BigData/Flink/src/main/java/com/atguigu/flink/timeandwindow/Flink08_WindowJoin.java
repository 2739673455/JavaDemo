package com.atguigu.flink.timeandwindow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink08_WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> orderEventDs = env.socketTextStream("localhost", 9990)
                .map(o -> new OrderEvent(Long.parseLong(o.split(" ")[0]), Long.parseLong(o.split(" ")[1])))
                .returns(Types.POJO(OrderEvent.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                                .withTimestampAssigner((o, l) -> o.getTs()));

        SingleOutputStreamOperator<OrderDetailEvent> orderDetailEventDs = env.socketTextStream("localhost", 9999)
                .map(o -> new OrderDetailEvent(Long.parseLong(o.split(" ")[0]), Long.parseLong(o.split(" ")[1])))
                .returns(Types.POJO(OrderDetailEvent.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetailEvent>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                                .withTimestampAssigner((o, l) -> o.getTs()));

        // 普通联结: 两条流的数据如果能进入同一个窗口，且满足join条件，就进行join
        orderEventDs
                .join(orderDetailEventDs)
                .where(OrderEvent::getId)
                .equalTo(OrderDetailEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L))) // 设定窗口
                .apply(
                        new JoinFunction<OrderEvent, OrderDetailEvent, String>() {
                            @Override
                            public String join(OrderEvent orderEvent, OrderDetailEvent orderDetailEvent) throws Exception {
                                return orderEvent.getId() + " - " + orderDetailEvent.getId();
                            }
                        }
                )
                .print();

        // 间隔联结: 一条流对另一条流一段间隔时间内的数据进行join，只支持事件时间语义，需要在keyBy()的基础上进行
        orderEventDs
                .keyBy(OrderEvent::getId)
                .intervalJoin(orderDetailEventDs.keyBy(OrderDetailEvent::getId))
                .between((Time.seconds(-2L)), Time.seconds(2L)) // 设定时间间隔，前后分别间隔2秒
                .upperBoundExclusive() // 排除上界
                .lowerBoundExclusive() // 排除下界
                .process(
                        new ProcessJoinFunction<OrderEvent, OrderDetailEvent, String>() {
                            @Override
                            public void processElement(OrderEvent orderEvent, OrderDetailEvent orderDetailEvent, ProcessJoinFunction<OrderEvent, OrderDetailEvent, String>.Context context, Collector<String> collector) throws Exception {
                                collector.collect(orderEvent.getId() + " - " + orderDetailEvent.getId());
                            }
                        }
                )
                .print();

        env.execute();
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
}
