package com.atguigu.flink.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
状态后端:
    1. HashMapStateBacked
        将状态存储在TaskManager的内存中，不适合大状态，读写性能高
    2. EmbeddedRocksDVStateBackend
        将状态存储在TaskManager的磁盘中，适合超大状态，读写性能较低
设置状态后端:
    1. 在代码中指定
        env.setStateBackend(new HashMapStateBackend())
        env.setStateBackend(new EmbeddedRocksDBStateBackend())
    2. 在flink-conf.yaml文件中指定
        state.backend.type: hashmap | rocksdb
    3. 在提交作业时通过参数指定
        bin/flink run -Dstate.backend=hashmap|rocksdb ...
* */
public class Flink10_StateBackend {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 5678);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.socketTextStream("localhost", 9999)
                .print();
        env.setParallelism(1);

        env.execute();
    }
}
