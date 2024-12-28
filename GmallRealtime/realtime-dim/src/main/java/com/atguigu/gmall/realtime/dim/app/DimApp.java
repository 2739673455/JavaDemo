package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.function.DimSinkFunction;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        // 创建环境
        // 配置检查点
        // 从Kafka中读取数据，获取kafkaDataStream
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // todo 4. 对流中数据进行类型转换并进行简单的ETL jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = getJsonObjectDS(kafkaStrDS);
        // todo 5. 使用FLinkCDC读取配置表的配置信息
        SingleOutputStreamOperator<TableProcessDim> configDS = getTableProcessDimDS(env);
        // todo 7. 根据配置表中配置信息，在HBase中建表或删表
        operateHBaseTable(configDS);
        // todo 8. 广播配置流 broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = configDS.broadcast(mapStateDescriptor);
        // todo 9. 将主流业务数据与广播流配置信息关联 connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjectDS.connect(broadcastDS);
        // todo 10. 对关联数据处理 process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(new TableProcessFunction(mapStateDescriptor));
        // todo 11. 将数据写入HBase
        dimDS.addSink(new DimSinkFunction());
    }

    // 获取主流数据，对数据进行过滤，转换为Json对象
    private static SingleOutputStreamOperator<JSONObject> getJsonObjectDS(DataStreamSource<String> kafkaStrDS) {
        return kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                        try {
                            JSONObject jsonObject = JSON.parseObject(s);
                            String db = jsonObject.getString("database");
                            String type = jsonObject.getString("type");
                            String data = jsonObject.getString("data");
                            if ("gmall".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2) {
                                collector.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            System.out.println("    脏数据");
                        }
                    }
                }
        );
    }

    // 获取配置表数据，转换为TableProcessDim对象
    private static SingleOutputStreamOperator<TableProcessDim> getTableProcessDimDS(StreamExecutionEnvironment env) {
        // 5.1 创建MySqlSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource("gmall_config", "table_process_dim");
        // 5.2 读取数据，封装为流
        DataStreamSource<String> mySqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
        // todo 6. 对配置流中数据进行类型转换 jsonStr -> 实体类对象
        return mySqlStrDS.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String s) {
                JSONObject jsonObject = JSON.parseObject(s);
                // 获取对配置表进行的操作类型
                String op = jsonObject.getString("op");
                TableProcessDim tableProcessDim;
                if ("d".equals(op)) {
                    // 删除配置，从before中获取删除前的配置
                    tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                } else {
                    // 增改查配置，从after获取配置信息
                    tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                }
                tableProcessDim.setOp(op);
                return tableProcessDim;
            }
        }).setParallelism(1);
    }

    // 根据配置流数据对HBase进行建表或删表操作
    private static void operateHBaseTable(SingleOutputStreamOperator<TableProcessDim> configDS) {
        configDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    Connection hbaseConnection;

                    @Override
                    public void open(Configuration parameters) {
                        hbaseConnection = HBaseUtil.getHbaseConnection();
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) {
                        String op = tableProcessDim.getOp();
                        String sinkTable = tableProcessDim.getSinkTable();
                        String[] families = tableProcessDim.getSinkFamily().split(",");
                        if ("r".equals(op) || "c".equals(op)) {
                            // 读取或添加配置，在HBase中创建表
                            HBaseUtil.createHBaseTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable, families);
                        } else if ("d".equals(op)) {
                            // 删除配置，从HBase删除表
                            HBaseUtil.dropHBaseTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable);
                        } else {
                            // 修改配置，先从HBase中删除对应的表，再创建新表
                            HBaseUtil.dropHBaseTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConnection, Constant.HBASE_NAMESPACE, sinkTable, families);
                        }
                        return tableProcessDim;
                    }

                    @Override
                    public void close() {
                        HBaseUtil.closeHBaseConnection(hbaseConnection);
                    }
                }
        ).setParallelism(1);
    }
}