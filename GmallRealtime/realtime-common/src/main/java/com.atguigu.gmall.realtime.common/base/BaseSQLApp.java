package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSQLApp {
    public void start(int port, int parallelism, String groupId) {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        handle(env, streamTableEnv);

    }

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv);

    public void readOdsDb(StreamTableEnvironment streamTableEnv, String groupId) {
        streamTableEnv.executeSql(
                "create table topic_db ( " +
                        "`database` string, " +
                        "`table` string, " +
                        "`type` string, " +
                        "`ts` bigint, " +
                        "`data` map<string,string>, " +
                        "`old` map<string,string>, " +
                        "process_time as proctime(), " +
                        "event_time as to_timestamp_ltz(ts,0), " +
                        "watermark for event_time as event_time " +
                        ") " +
                        SQLUtil.getKafkaDDL(Constant.TOPIC_DB, groupId)
        );
    }

    public void readDimDic(StreamTableEnvironment streamTableEnv) {
        streamTableEnv.executeSql(
                "create table base_dic ( " +
                        "dic_code string, " +
                        "info row<dic_name string>, " +
                        "primary key(dic_code) not enforced " +
                        ") " +
                        SQLUtil.getHBaseDDL("dim_base_dic")
        );
    }
}
