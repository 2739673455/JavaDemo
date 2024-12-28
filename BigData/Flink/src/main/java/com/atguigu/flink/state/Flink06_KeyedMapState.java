package com.atguigu.flink.state;

/*
按键分区状态
    Map状态 MapState
        UV get(UK var1)  获取指定key对应值
        void put(UK var1, UV var2)  将指定key的value添加到状态中
        void putAll(Map<UK, UV> var1)  将指定的Map集合中的数据添加到状态中
        void remove(UK var1)  从状态中移除指定key对应的值
        boolean contains(UK var1)  判断是否包含key
        Iterable<Map.Entry<UK, UV>> entries()  获取所有entry
        Iterable<UK> keys()  获取所有key
        Iterable<UV> values()  获取所有value
        Iterator<Map.Entry<UK, UV>> iterator()  迭代
        boolean isEmpty()  判断是否为空
        void clear()  清空状态
* */

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Flink06_KeyedMapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 去掉每种传感器重复水位值
        env.socketTextStream("localhost", 9999)
                .map(o -> new WaterSensor(o.split(" ")[0], Long.parseLong(o.split(" ")[1]), Long.parseLong(o.split(" ")[2])))
                .keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 声明状态
                            private MapState<Long, Long> state;

                            // 生命周期方法，初始化状态
                            @Override
                            public void open(Configuration parameters) {
                                RuntimeContext context = getRuntimeContext();
                                MapStateDescriptor<Long, Long> stateDescriptor = new MapStateDescriptor<>("waterSensor", Types.LONG, Types.LONG);
                                state = context.getMapState(stateDescriptor);
                            }

                            // 使用状态
                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                // 将当前水位值添加进状态
                                state.put(waterSensor.getVc(), null);
                                collector.collect("传感器" + waterSensor.getId() + "水位值:" + state.keys());
                            }
                        }
                )
                .print();

        env.execute();
    }
}
