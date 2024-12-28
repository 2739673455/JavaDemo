package com.atguigu.flink.state;

/*
按键分区状态
    归约状态 ReducingState
        OUT get()  获取状态值
        void add(IN var1)  将数据添加到状态中
        void clear()  清空状态值
* */

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_KeyedReducingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 求每种传感器水位和
        env.socketTextStream("localhost", 9999)
                .map(o -> new WaterSensor(o.split(" ")[0], Long.parseLong(o.split(" ")[1]), Long.parseLong(o.split(" ")[2])))
                .keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 声明状态
                            private ReducingState<Long> state;

                            // 生命周期方法，初始化状态
                            @Override
                            public void open(Configuration parameters) {
                                RuntimeContext context = getRuntimeContext();
                                ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<>(
                                        "waterSensor",
                                        new ReduceFunction<Long>() {
                                            @Override
                                            public Long reduce(Long result, Long in) {
                                                return result + in;
                                            }
                                        },
                                        Types.LONG
                                );
                                state = context.getReducingState(stateDescriptor);
                            }

                            // 使用状态
                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                // 将当前水位值添加进状态
                                state.add(waterSensor.getVc());
                                collector.collect("传感器" + waterSensor.getId() + "水位加和:" + state.get());
                            }
                        }
                )
                .print();

        env.execute();
    }
}
