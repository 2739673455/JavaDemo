package com.atguigu.flink.state;

/*
按键分区状态
    聚合状态 AggregatingState
        OUT get()  获取状态值
        void add(IN var1)  将数据添加到状态中
        void clear()  清空状态值
* */

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink08_KeyedAggregatingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 求每种传感器水位平均值
        env.socketTextStream("localhost", 9999)
                .map(o -> new WaterSensor(o.split(" ")[0], Long.parseLong(o.split(" ")[1]), Long.parseLong(o.split(" ")[2])))
                .keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 声明状态
                            private AggregatingState<Long, Double> state;

                            // 生命周期方法，初始化状态
                            @Override
                            public void open(Configuration parameters) {
                                RuntimeContext context = getRuntimeContext();
                                AggregatingStateDescriptor<Long, Tuple2<Long, Long>, Double> stateDescriptor = new AggregatingStateDescriptor<>(
                                        "waterSensor",
                                        new AggregateFunction<Long, Tuple2<Long, Long>, Double>() {
                                            @Override
                                            public Tuple2<Long, Long> createAccumulator() {
                                                return Tuple2.of(0L, 0L);
                                            }

                                            @Override
                                            public Tuple2<Long, Long> add(Long in, Tuple2<Long, Long> out) {
                                                return Tuple2.of(in + out.f0, out.f1 + 1);
                                            }

                                            @Override
                                            public Double getResult(Tuple2<Long, Long> out) {
                                                return (double) out.f0 / out.f1;
                                            }

                                            @Override
                                            public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                                                return null;
                                            }
                                        },
                                        Types.TUPLE(Types.LONG, Types.DOUBLE)
                                );
                                state = context.getAggregatingState(stateDescriptor);
                            }

                            // 使用状态
                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                // 将当前水位值添加进状态
                                state.add(waterSensor.getVc());
                                collector.collect("传感器" + waterSensor.getId() + "水位平均值:" + state.get());
                            }
                        }
                )
                .print();

        env.execute();
    }
}
