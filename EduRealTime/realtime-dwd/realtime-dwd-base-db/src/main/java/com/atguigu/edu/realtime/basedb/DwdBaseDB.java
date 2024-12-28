package com.atguigu.edu.realtime.basedb;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.bean.TableProcessDwd;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import com.atguigu.edu.common.util.FlinkSourceUtil;
import com.atguigu.edu.common.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DwdBaseDB extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDB().start(10011, 4, "dwd_base_db", "topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String in, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(in);
                    if (!jsonObject.getString("type").startsWith("bootstrap")) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception ignored) {
                }
            }
        });

        // Mysql CDC 从配置表读取 dwd 配置信息
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource("edu_config", "table_process_dwd");
        SingleOutputStreamOperator<TableProcessDwd> configDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "config").setParallelism(1)
                .map(
                        new MapFunction<String, TableProcessDwd>() {
                            @Override
                            public TableProcessDwd map(String in) throws Exception {
                                JSONObject jsonObject = JSONObject.parseObject(in);
                                String op = jsonObject.getString("op");
                                TableProcessDwd tableProcessDwd;
                                if ("delete".equals(op)) {
                                    tableProcessDwd = jsonObject.getObject("before", TableProcessDwd.class);
                                } else {
                                    tableProcessDwd = jsonObject.getObject("after", TableProcessDwd.class);
                                }
                                tableProcessDwd.setOp(op);
                                return tableProcessDwd;
                            }
                        }
                ).setParallelism(1);

        // 广播处理
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("config", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcast = configDS.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonDS.connect(broadcast);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> result = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    Map<String, TableProcessDwd> configMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection mySqlConnection = JdbcUtil.getMySqlConnection();
                        List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(mySqlConnection, "select * from edu_config.table_process_dwd", TableProcessDwd.class, true);
                        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
                            String key = tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType();
                            configMap.put(key, tableProcessDwd);
                        }
                        JdbcUtil.closeMySqlConnection(mySqlConnection);
                    }

                    @Override
                    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        String op = tableProcessDwd.getOp();
                        String key = tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType();
                        if ("d".equals(op)) {
                            broadcastState.remove(key);
                            configMap.remove(key);
                        } else {
                            broadcastState.put(key, tableProcessDwd);
                        }
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        String key = jsonObject.getString("table") + ":" + jsonObject.getString("type");
                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        TableProcessDwd tableProcessDwd;
                        if ((tableProcessDwd = broadcastState.get(key)) != null || (tableProcessDwd = configMap.get(key)) != null) {
                            JSONObject data = jsonObject.getJSONObject("data");
                            List<String> columnsList = Arrays.asList(tableProcessDwd.getSinkColumns().split(","));
                            data.entrySet().removeIf(o -> !columnsList.contains(o.getKey()));
                            data.put("ts", jsonObject.getLong("ts") * 1000);
                            collector.collect(Tuple2.of(data, tableProcessDwd));
                        }
                    }
                }
        );
        // 数据分流，写入Kafka
        result.print();
        result.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}
