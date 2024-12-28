package com.atguigu.flink.state;

/*
按键分区状态
    列表状态 ListState
        void update(List<T> values)  将传入的list集合中的数据更新到状态中，会覆盖状态中已有的数据
        void addAll(List<T> values)  将传入的list集合中的数据添加到状态中
        OUT get()  获取状态中的数据
        void add(IN value)  将传入的值添加到状态中
        void clear()  清空状态

* */

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Flink05_KeyedListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 针对每种传感器输出最高的3个水位值
        env.socketTextStream("localhost", 9999)
                .map(o -> new WaterSensor(o.split(" ")[0], Long.parseLong(o.split(" ")[1]), Long.parseLong(o.split(" ")[2])))
                .keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 声明状态
                            private ListState<Long> state;

                            // 生命周期方法，初始化状态
                            @Override
                            public void open(Configuration parameters) {
                                RuntimeContext context = getRuntimeContext();
                                ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>("waterSensor", Types.LONG);
                                state = context.getListState(stateDescriptor);
                            }

                            // 使用状态
                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                // 将当前水位值添加进状态
                                state.add(waterSensor.getVc());
                                ArrayList<Long> vcList = new ArrayList<>();
                                state.get().forEach(vcList::add);
                                vcList.sort((vc1, vc2) -> -Long.compare(vc1, vc2));
                                if (vcList.size() > 3) {
                                    vcList.remove(vcList.size() - 1);
                                }
                                state.update(vcList);
                                collector.collect("传感器" + waterSensor.getId() + "最高的3个水位值:" + vcList);
                            }
                        }
                )
                .print();

        env.execute();
    }
}
