package com.atguigu.flink.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/*
算子状态:
    广播状态: 一般不会用于广播被计算的数据，而是用于广播一些少量的通用的全局的配置
        在某个算子中，根据某个配置来决定执行哪种逻辑
        使用广播状态灵活修改配置，且要生效到某个算子所有并行实例中
* */

public class Flink03_OperatorBroadcastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启检查点
        env.enableCheckpointing(2000L);

        // 数据流
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9999);

        // 配置流
        DataStreamSource<String> configDs = env.socketTextStream("localhost", 9990);
        // 处理成广播流
        MapStateDescriptor<String, Integer> broadcastDescriptor = new MapStateDescriptor<>("broadcast", String.class, Integer.class);
        BroadcastStream<String> broadcast = configDs.broadcast(broadcastDescriptor);

        // 将数据流和广播流进行connect
        ds.connect(broadcast)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    Integer flag;

                    @Override
                    public void processElement(String s, BroadcastProcessFunction<String, String, String>.ReadOnlyContext context, Collector<String> collector) throws Exception {
                        // 从广播状态中获取配置
                        ReadOnlyBroadcastState<String, Integer> broadcastState = context.getBroadcastState(broadcastDescriptor);
                        flag = broadcastState.get("flag") == null ? 0 : broadcastState.get("flag");
                        if (flag == 1)
                            System.out.println("1 -> ");
                        else if (flag == 2)
                            System.out.println("2 -> ");
                        else
                            System.out.println("0 -> ");
                        collector.collect(System.currentTimeMillis() + " : " + s);
                    }

                    @Override
                    public void processBroadcastElement(String s, BroadcastProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
                        // 获取广播状态
                        BroadcastState<String, Integer> broadcastState = context.getBroadcastState(broadcastDescriptor);
                        broadcastState.put("flag", Integer.parseInt(s));
                    }
                })
                .print();

        env.execute();
    }
}
