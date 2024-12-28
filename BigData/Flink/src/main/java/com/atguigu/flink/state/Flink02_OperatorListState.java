package com.atguigu.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/*
算子状态:
    列表状态 ListState: 可以作为List使用，但所有和状态相关的操作都是Flink管理
    联合列表状态 UnionListState: 可以作为List使用，但所有和状态相关的操作都是Flink管理

    联合列表状态和列表状态的区别:
        从状态基本使用上来说没有明显区别
        状态还原时，列表状态与作业失败前一样，联合列表状态是将算子的所有并行实例的状态联合到一起再给到每个算子的并行实例
    联合列表状态使用场景:
        KafkaSource: Flink的KafkaSource算子会记录自己的状态，就是读取kafka的某个主题某个分区到哪个offset
            作业失败前:
                Kafka-TopicA-partition0  Flink-parallelism0  100
                Kafka-TopicA-partition1  Flink-parallelism1  102
                Kafka-TopicA-partition2  Flink-parallelism2  99
                Kafka-TopicA-partition3  Flink-parallelism3  101
            作业重启后:
                Kafka-TopicA-partition0  Flink-parallelism2  [Kafka-TopicA-p0:100, p1:102, p2:99, p3:101]
                Kafka-TopicA-partition1  Flink-parallelism0  [Kafka-TopicA-p0:100, p1:102, p2:99, p3:101]
                Kafka-TopicA-partition2  Flink-parallelism3  [Kafka-TopicA-p0:100, p1:102, p2:99, p3:101]
                Kafka-TopicA-partition3  Flink-parallelism1  [Kafka-TopicA-p0:100, p1:102, p2:99, p3:101]
* */

public class Flink02_OperatorListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置程序重启策略
        //env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(1)));
        // 开启检查点，程序会无限重启
        env.enableCheckpointing(2000L);

        env.socketTextStream("localhost", 9999)
                .map(new AMapFunction()).setParallelism(2)
                .addSink(
                        new SinkFunction<String>() {
                            @Override
                            public void invoke(String value, Context context) {
                                if (value.contains("100"))
                                    throw new RuntimeException("100异常");
                                System.out.println(value);
                            }
                        }
                );
        env.execute();
    }

    public static class AMapFunction implements MapFunction<String, String>, CheckpointedFunction {
        // 列表状态
        private ListState<String> listState;

        @Override
        public String map(String s) throws Exception {
            listState.add(s);
            return listState.get().toString();
        }

        // 状态快照，该方法伴随检查点执行
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
            System.out.println("snapshotState...");
        }

        // 初始化状态
        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            OperatorStateStore operatorStateStore = functionInitializationContext.getOperatorStateStore();
            ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("listState_1", String.class);
            //listState = operatorStateStore.getListState(listStateDescriptor); // 获取列表状态
            listState = operatorStateStore.getUnionListState(listStateDescriptor); // 获取联合列表状态
        }
    }
}
