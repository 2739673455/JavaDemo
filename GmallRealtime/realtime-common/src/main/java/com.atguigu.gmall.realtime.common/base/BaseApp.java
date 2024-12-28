package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
使用DataStreamAPI开发Flink应用程序基类
模板设计模式
    在父类中定义完成某一个功能的核心算法骨架(步骤)，但某些步骤在父类中无法实现，交给子类实现
    好处: 定义了模板，在不改变父类核心算法骨架的前提下，每个子类都可以有不同的实现
* */
public abstract class BaseApp {
    public void start(int port, int parallelism, String checkpointPathAndGroupId, String topic) {
        // 1. 环境准备
        // 1.1 流处理环境
        Configuration configuration = new Configuration().set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // 1.2 并行度
        env.setParallelism(parallelism);

        // 2. 检查点相关设置
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置检查点超时时间
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000L);
        // 2.3 设置job取消后检查点是否保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 设置两个检查点之间最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // 2.5 设置重启策略
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        // 2.6 设置状态后端 hashMap状态后端 状态-TaskManager堆内存  检查点-JobManager堆内存
        env.setStateBackend(new HashMapStateBackend());
        // 2.7 设置检查点存储路径
        //checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink-checkpoint" + checkpointPathAndGroupId);
        // 2.8 设置操作hadoop的用户
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 3. 从kafka读取数据
        // 3.1 声明消费的主题以及消费者组
        // 3.2 创建KafkaSource对象
        // Flink从Kafka读取数据保证一致性: KafkaSource -> KafkaSourceReader -> offsetsToCommit(算子状态)
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, checkpointPathAndGroupId);
        // 3.3 消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        // 4. 业务处理
        handle(env, kafkaStrDS);
        // 5. 提交作业
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);
}
