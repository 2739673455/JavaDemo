package com.atguigu.edu.common.base;

import com.atguigu.edu.common.util.FlinkSourceUtil;
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

public abstract class BaseApp {
    public void start(int port, int parallelism, String checkpointPathAndGroupId, String topic) {
        Configuration configuration = new Configuration().set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);

        // 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点超时时间
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000L);
        // 设置job取消后检查点是否保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置两个检查点之间最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // 设置重启策略
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        // 设置状态后端 hashMap状态后端 状态-TaskManager堆内存  检查点-JobManager堆内存
        env.setStateBackend(new HashMapStateBackend());
        // 设置检查点存储路径
        //checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink-checkpoint" + checkpointPathAndGroupId);
        // 设置操作hadoop的用户
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, checkpointPathAndGroupId);
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        handle(env, kafkaStrDS);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);
}
