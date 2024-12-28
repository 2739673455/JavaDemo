package com.atguigu.edu.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.bean.TableProcessDim;
import com.atguigu.edu.common.constant.Constant;
import com.atguigu.edu.common.util.FlinkSourceUtil;
import com.atguigu.edu.common.util.HBaseUtil;
import com.atguigu.edu.common.util.JdbcUtil;
import com.atguigu.edu.common.util.RedisUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001, 4, "dim", "topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 从 Kafka 读取 topic_db 数据
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaStrDS.map(JSON::parseObject)
                .filter(
                        (FilterFunction<JSONObject>) o -> {
                            String database = o.getString("database");
                            String type = o.getString("type");
                            JSONObject data = o.getJSONObject("data");
                            return "edu".equals(database)
                                    && Arrays.asList("insert", "update", "delete", "bootstrap-insert").contains(type)
                                    && data != null && !data.isEmpty();
                        }
                );

        // Mysql CDC 从配置表读取 dim 配置信息
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource("edu_config", "table_process_dim");
        SingleOutputStreamOperator<TableProcessDim> configDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "table_process_dim").setParallelism(1)
                .map(
                        (MapFunction<String, TableProcessDim>) in -> {
                            JSONObject jsonObj = JSON.parseObject(in);
                            String op = jsonObj.getString("op");
                            new TableProcessDim();
                            TableProcessDim tableProcessDim;
                            if ("d".equals(op)) {
                                tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                            } else {
                                tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                            }
                            tableProcessDim.setOp(op);
                            return tableProcessDim;
                        }
                ).setParallelism(1)
                // 根据配置在 HBase 中建表或删表
                .map(
                        new RichMapFunction<TableProcessDim, TableProcessDim>() {
                            Connection hbaseConnection;

                            @Override
                            public void open(Configuration parameters) {
                                hbaseConnection = HBaseUtil.getHbaseConnection();
                            }

                            @Override
                            public void close() {
                                HBaseUtil.closeHBaseConnection(hbaseConnection);
                            }

                            @Override
                            public TableProcessDim map(TableProcessDim in) {
                                String op = in.getOp();
                                String sinkTable = in.getSinkTable();
                                String[] families = in.getSinkFamily().split(",");
                                if (Arrays.asList("r", "c").contains(op)) {
                                    HBaseUtil.createHBaseTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable, families);
                                } else if ("d".equals(op)) {
                                    HBaseUtil.dropHBaseTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable);
                                } else {
                                    HBaseUtil.dropHBaseTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable);
                                    HBaseUtil.createHBaseTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable, families);
                                }
                                return in;
                            }
                        }
                ).setParallelism(1);

        // 广播流
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("configMap", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = configDS.broadcast(mapStateDescriptor);

        // 广播配置流和主流关联
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonDS.connect(broadcastDS);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
                    Map<String, TableProcessDim> configMap = new HashMap<>();

                    // 从Mysql预加载配置表信息，解决主流数据先到，广播数据后到的问题
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        java.sql.Connection mySqlConnection = JdbcUtil.getMySqlConnection();
                        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mySqlConnection, "select * from edu_config.table_process_dim", TableProcessDim.class, true);
                        for (TableProcessDim tableProcessDim : tableProcessDimList) {
                            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                        }
                        JdbcUtil.closeMySqlConnection(mySqlConnection);
                    }

                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        String sourceTable = tableProcessDim.getSourceTable();
                        String op = tableProcessDim.getOp();
                        if ("d".equals(op)) {
                            broadcastState.remove(sourceTable);
                            configMap.remove(sourceTable);
                        } else {
                            broadcastState.put(sourceTable, tableProcessDim);
                        }
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                        String table = jsonObject.getString("table");
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        TableProcessDim tableProcessDim;
                        if ((tableProcessDim = broadcastState.get(table)) != null || (tableProcessDim = configMap.get(table)) != null) {
                            JSONObject data = jsonObject.getJSONObject("data");
                            // 过滤字段
                            List<String> columnList = Arrays.asList(tableProcessDim.getSinkColumns().split(","));
                            data.entrySet().removeIf(entry -> !columnList.contains(entry.getKey()));
                            // 补充对业务数据库维度表进行的操作类型
                            data.put("type", jsonObject.getString("type"));
                            collector.collect(Tuple2.of(data, tableProcessDim));
                        }
                    }
                }
        );
        resultDS.addSink(
                new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
                    Connection hbaseConnection;
                    Jedis jedis;

                    @Override
                    public void open(Configuration parameters) {
                        hbaseConnection = HBaseUtil.getHbaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() {
                        HBaseUtil.closeHBaseConnection(hbaseConnection);
                        RedisUtil.close(jedis);
                    }

                    @Override
                    public void invoke(Tuple2<JSONObject, TableProcessDim> in, Context context) {
                        JSONObject jsonObject = in.f0;
                        TableProcessDim tableProcessDim = in.f1;
                        String type = jsonObject.getString("type");
                        String sinkTable = tableProcessDim.getSinkTable();
                        String sinkFamily = tableProcessDim.getSinkFamily();
                        String rowKey = jsonObject.getString(tableProcessDim.getSinkRowKey());
                        jsonObject.remove("type");
                        if ("delete".equals(type)) {
                            HBaseUtil.deleteRow(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
                        } else {
                            HBaseUtil.putRow(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable, rowKey, sinkFamily, jsonObject);
                        }
                        jedis.del(sinkTable + ":" + rowKey);
                    }
                }
        );
    }
}