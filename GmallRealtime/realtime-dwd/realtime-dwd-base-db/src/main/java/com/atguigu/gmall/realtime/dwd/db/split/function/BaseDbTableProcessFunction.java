package com.atguigu.gmall.realtime.dwd.db.split.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {
    private MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;
    private Map<String, TableProcessDwd> configMap = new HashMap<>();

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection mySqlConnection = JdbcUtil.getMySqlConnection();
        List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(mySqlConnection, "select * from gmall_config.table_process_dwd", TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
            String sourceTable = tableProcessDwd.getSourceTable();
            String sourceType = tableProcessDwd.getSourceType();
            String key = sourceTable + ":" + sourceType;
            configMap.put(key, tableProcessDwd);
        }
        JdbcUtil.closeMySqlConnection(mySqlConnection);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        if ("delete".equals(type)) return;
        String key = table + ":" + type;
        ReadOnlyBroadcastState<String, TableProcessDwd> state = context.getBroadcastState(mapStateDescriptor);
        TableProcessDwd tableProcessDwd;
        if ((tableProcessDwd = state.get(key)) != null || (tableProcessDwd = configMap.get(key)) != null) {
            JSONObject data = jsonObject.getJSONObject("data");
            String columns = tableProcessDwd.getSinkColumns();
            deleteNotNeedColumns(data, columns);
            data.put("ts", jsonObject.getString("ts"));
            collector.collect(Tuple2.of(data, tableProcessDwd));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        String op = tableProcessDwd.getOp();
        String sourceTable = tableProcessDwd.getSourceTable();
        String sourceType = tableProcessDwd.getSourceType();
        String key = sourceTable + ":" + sourceType;
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
        if ("d".equals(op)) {
            broadcastState.remove(key);
            configMap.remove(key);
        } else {
            broadcastState.put(key, tableProcessDwd);
        }
    }

    private void deleteNotNeedColumns(JSONObject data, String columns) {
        List<String> columnsList = Arrays.asList(columns.split(","));
        Set<Map.Entry<String, Object>> dataEntry = data.entrySet();
        dataEntry.removeIf(column -> !columnsList.contains(column.getKey()));
    }
}

