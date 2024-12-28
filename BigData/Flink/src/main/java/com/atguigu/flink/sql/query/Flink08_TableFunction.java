package com.atguigu.flink.sql.query;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class Flink08_TableFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9999);
        Table table = streamTableEnv.fromDataStream(ds);
        streamTableEnv.createTemporaryView("t1", table);
        // 注册函数
        streamTableEnv.createTemporaryFunction("splitfunc", SplitFunc.class);
        // TableAPI
        streamTableEnv.from("t1")
                .joinLateral(call("splitfunc", $("f0"), ","))
                .select($("f0"), $("word"), $("len"))
                .execute().print();
        // SQL
        streamTableEnv.sqlQuery("select f0, word, len from t1 left join lateral table(splitfunc(f0,',')) on true").execute().print();
    }

    // 分隔字符串，将一个字符串转换为(字符串，长度)的row
    @FunctionHint(output = @DataTypeHint(value = "ROW<word string, len integer>"))
    public static class SplitFunc extends TableFunction<Row> {
        public void eval(String in, String seperator) {
            for (String word : in.split(seperator)) {
                collect(Row.of(word, word.length()));
            }
        }
    }
}