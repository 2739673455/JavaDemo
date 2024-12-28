package com.atguigu.flink.datastreamapi.sink;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/*
添加依赖:
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-jdbc</artifactId>
        <version>3.1.0-1.17</version>
        <scope>compile</scope>
    </dependency>
    <dependency>
        <groupId>com.mysql</groupId>
        <artifactId>mysql-connector-j</artifactId>
        <version>8.0.32</version>
        <scope>compile</scope>
    </dependency>

JDBCConnector: 支持将数据写入外部数据库
 * */

public class Flink04_JDBCConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(2000L);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(2), WatermarkStrategy.noWatermarks(), "ds");
        SinkFunction<Event> jdbcSink = JdbcSink.sink(
                "insert into event_table1 (user,url,ts) values(?,?,?)",
                //"replace into event_table1 (user,url,ts) values(?,?,?)", // 覆盖该条数据
                //"insert into event_table1 (user,url,ts) values(?,?,?) on duplicate key update url=values(url)", // 覆盖该条数据指定字段
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        // 给sql中的占位符赋值
                        preparedStatement.setString(1, event.getUser());
                        preparedStatement.setString(2, event.getUrl());
                        preparedStatement.setLong(3, event.getTs());
                    }
                },
                // 按批写入
                JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(5000L)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:3306/db1")
                        .withUsername("root")
                        .withPassword("123321")
                        .build()
        );
        ds.addSink(jdbcSink);

        env.execute();
    }
}
