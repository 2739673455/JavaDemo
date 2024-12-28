package com.atguigu.flink.checkpoint;

import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Flink01_CheckPointConfig {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8888);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(2000L);
        // 获取检查点配置对象
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 检查点模式
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        // 检查点周期
        checkpointConfig.setCheckpointInterval(5000L);
        // 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 检查点存储
        //  将检查点存储到JobManager的内存中
        checkpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage());
        //  将检查点存储到HDFS中
        //   设置Hadoop用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://hadoop102:8020/flink-checkpoint"));
        // 检查点超时时间
        checkpointConfig.setCheckpointTimeout(900000L);
        // 同时启动的检查点个数
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        // 相邻检查点之间最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(3000L);
        // 作业取消后检查点是否保留
        //checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION); // 删除
        // 开启非对齐检查点，开启后同时启动的检查点只能有1个
        checkpointConfig.enableUnalignedCheckpoints();
        // 强制开启非对齐检查点
        checkpointConfig.setForceUnalignedCheckpoints(true);
        // 检查点允许失败的次数
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        // 对齐Barrier的超时时间
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofMinutes(5L));
        // 最终检查点
        configuration.setBoolean(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        //开启changelog
        env.enableChangelogStateBackend(true);

        env.fromSource(SourceUtil.getDataGeneratorSource(1), WatermarkStrategy.noWatermarks(), "ds")
                .print();

        env.execute();
    }
}
