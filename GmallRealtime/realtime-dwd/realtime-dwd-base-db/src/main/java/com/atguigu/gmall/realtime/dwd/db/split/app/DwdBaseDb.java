package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.dwd.db.split.function.BaseDbTableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/*
工具域优惠券领取事务事实表
工具域优惠券使用（下单+支付）事务事实表
互动域收藏商品事务事实表
用户域用户注册事务事实表
 * */

public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(10019, 4, "dwd_base_db", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        // 对流中数据进行类型转换和ETL jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjectDs = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            String type = jsonObject.getString("type");
                            if (!type.startsWith("bootstrap-")) {
                                collector.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            System.out.println("数据不是json格式");
                        }
                    }
                }
        );

        // 使用FlinkCDC读取配置表的信息
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource("gmall_config", "table_process_dwd");
        SingleOutputStreamOperator<TableProcessDwd> configDs = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource").setParallelism(1)
                .map(
                        new MapFunction<String, TableProcessDwd>() {
                            @Override
                            public TableProcessDwd map(String s) {
                                JSONObject jsonObject = JSON.parseObject(s);
                                String op = jsonObject.getString("op");
                                TableProcessDwd tableProcessDwd;
                                if ("d".equals(op))
                                    tableProcessDwd = jsonObject.getObject("before", TableProcessDwd.class);
                                else
                                    tableProcessDwd = jsonObject.getObject("after", TableProcessDwd.class);
                                tableProcessDwd.setOp(op);
                                return tableProcessDwd;
                            }
                        }
                ).setParallelism(1);

        // 对配置流广播，将主流业务数据和广播流中的配置信息关联，对关联后的数据进行处理
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDs = configDs.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDs = jsonObjectDs.connect(broadcastDs);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultDs = connectDs.process(new BaseDbTableProcessFunction(mapStateDescriptor));

        // 将分流后数据写入Kafka
        resultDs.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}
