package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.*;

public class Flink06_ScalarFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<WaterSensor> ds = env.fromSource(SourceUtil.getWaterSensorGeneratorSource(2), WatermarkStrategy.noWatermarks(), "ds");
        Table table = streamTableEnv.fromDataStream(ds);
        streamTableEnv.createTemporaryView("t1", table);
        // 注册函数
        streamTableEnv.createTemporaryFunction("upperfunc", UpperFunc.class);
        // TableAPI
        table.select($("id"), call("upperfunc", "abcd")).execute().print();
        // SQL
        streamTableEnv.sqlQuery("select upperfunc('abcd') from t1").execute().print();
    }

    public static class UpperFunc extends ScalarFunction {
        public String eval(String o) {
            return o.toUpperCase();
        }
    }
}
