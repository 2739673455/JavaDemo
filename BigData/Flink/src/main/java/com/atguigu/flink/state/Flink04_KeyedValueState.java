package com.atguigu.flink.state;

/*
按键分区状态
    值状态 ValueState
        T value()  获取状态值
        void update(T value)  更新状态值
        void clear()  清空状态值
* */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink04_KeyedValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 检测每种传感器水位值，若连续两个水位值差值大于10，则报警
        env.socketTextStream("localhost", 9999)
                .map(o -> new WaterSensor(o.split(" ")[0], Long.parseLong(o.split(" ")[1]), Long.parseLong(o.split(" ")[2])))
                .keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 声明状态
                            private ValueState<Long> state;

                            // 生命周期方法，初始化状态
                            @Override
                            public void open(Configuration parameters) {
                                RuntimeContext context = getRuntimeContext();
                                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("waterSensor", Long.class);
                                state = context.getState(stateDescriptor);
                            }

                            // 使用状态
                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                // 获取当前水位
                                long vc = waterSensor.getVc();
                                Long lastVc = state.value();
                                if (lastVc != null && Math.abs(lastVc - vc) > 10) {
                                    collector.collect("警告: " + "lastVc: " + lastVc + "vc: " + vc);
                                }
                                state.update(vc);
                            }
                        }
                )
                .print();

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class WaterSensor {
        private String id;
        private long vc;
        private long ts;
    }
}
