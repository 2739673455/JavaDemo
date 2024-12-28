package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
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

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String, TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 将配置表中配置信息预加载，解决主流数据先到，广播数据后到的问题
        Connection mySqlConnection = JdbcUtil.getMySqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mySqlConnection, "select * from gmall_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeMySqlConnection(mySqlConnection);
    }

    @Override
    // processElement 处理主流数据 根据当前处理的数据的表名到广播状态中获取对应的配置信息，如果获取得到说明是维度，传递到下游，否则不做处理
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        String tableName = jsonObject.getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
        TableProcessDim tableProcessDim;
        // 如果是维度表
        if ((tableProcessDim = broadcastState.get(tableName)) != null || (tableProcessDim = configMap.get(tableName)) != null) {
            JSONObject dataJsonObject = jsonObject.getJSONObject("data");
            String sinkColumns = tableProcessDim.getSinkColumns();
            // 过滤掉不需要的字段
            deleteNotNeedColumns(dataJsonObject, sinkColumns);
            // 补充对业务数据库维度表进行的操作类型
            dataJsonObject.put("type", jsonObject.getString("type"));
            collector.collect(Tuple2.of(dataJsonObject, tableProcessDim));
        }
    }

    @Override
    // processBroadcastElement 处理广播流数据 将配置信息放入广播状态 k:表名 v:实体类对象
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String sourceTable = tableProcessDim.getSourceTable();
        // 获取对配置表进行操作的类型
        String op = tableProcessDim.getOp();
        if ("d".equals(op)) {
            // 将广播状态中对应的配置信息删除
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            // 将读取、添加、更新的数据更新到广播状态中
            broadcastState.put(sourceTable, tableProcessDim);
            configMap.put(sourceTable, tableProcessDim);
        }
    }

    public void deleteNotNeedColumns(JSONObject jsonObject, String sinkColumns) {
        List<String> columns = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();
        entries.removeIf(entry -> !columns.contains(entry.getKey()));
    }
}
