package com.atguigu.flink.architecture;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
Flink的数据分发规则(分区规则):
    ChannelSelector
        StreamPartitioner

          KeyGroupStreamPartitioner : keyBy()
             按照指定的key求hash值，对下游算子并行度取余，决定数据发往哪个并行实例

          RebalancePartitioner : rebalance()
             轮询发往下一个并行实例
             若上下游并行度不一样，且未指定数据分发规则，默认为rebalance()

          RescalePartitioner : rescale()
             按轮询，将数据均衡的发往组内的下游的并行实例
             0->0
             0->1
             ----
             1->2
             1->3

          ShufflePartitioner : shuffle()
             随机

          BroadcastPartitioner : broadcast()
             广播

          GlobalPartitioner : global()
             强制并行度为1

          ForwardPartitioner : forward()
             直连，上游与下游并行度保持一直
             若上下游并行度一样，且未指定数据分发规则，默认为forward()

算子链:
     将上下游算子的并行实例合并在一起，形成一个算子链
合并算子链的条件:
     1. 上下游算子的并行度一致
     2. 数据分发规则必须为forward()
为何合并算子链:
     减少线程间切换、缓冲的开销，并在减少延迟的同时增加整体吞吐量
全局禁用算子链合并
     env.disableOperatorChaining()
从当前算子开始开启新链(和下游算子链合并)
     startNewChain()
禁用当前算子链合并(不和上下游算子合并)
     disableChaining()
 * */
public class Flink02_OperatorChain {
    public static void main(String[] args) throws Exception {
        //创建配置对象
        Configuration configuration = new Configuration();
        //指定webui的地址和端口
        configuration.setString("rest.address", "127.0.0.1");
        configuration.setInteger("rest.port", 8008);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.socketTextStream("localhost", 9999)
                .map(s -> s)
                .keyBy(s -> s)
                .map(s -> s)
                .rebalance()
                .map(s -> s)
                .rescale()
                .map(s -> s)
                .shuffle()
                .map(s -> s)
                .broadcast()
                .map(s -> s)
                .global()
                .map(s -> s)
                .forward()
                .map(s -> s)
                .print();
        env.execute();
    }
}
