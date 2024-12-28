package com.atguigu.edu.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseUtil {
    // 获取HBase连接
    public static Connection getHbaseConnection() {
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZK_QUORUM);
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 关闭HBase连接
    public static void closeHBaseConnection(Connection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // 在HBase建表
    public static void createHBaseTable(Connection connection, String namespace, String tableName, String... families) {
        if (families.length < 1) {
            System.out.println("families is empty");
            return;
        }
        try (Admin admin = connection.getAdmin()) {
            TableName tableNameObject = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObject)) {
                System.out.println(namespace + ":" + tableName + " is exist");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObject);
            for (String family : families) {
                tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build());
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println(namespace + ":" + tableName + " is created");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 从HBase删表
    public static void dropHBaseTable(Connection connection, String namespace, String tableName) {
        try (Admin admin = connection.getAdmin()) {
            TableName tableNameObject = TableName.valueOf(namespace, tableName);
            if (!admin.tableExists(tableNameObject)) {
                System.out.println(namespace + ":" + tableName + " is not exist");
                return;
            }
            admin.disableTable(tableNameObject);
            admin.deleteTable(tableNameObject);
            System.out.println(namespace + ":" + tableName + " is deleted");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 向HBase写数据
    public static void putRow(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject jsonObject) {
        try (Table table = connection.getTable(TableName.valueOf(namespace, tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            for (String columnName : jsonObject.keySet()) {
                String columnValue = jsonObject.getString(columnName);
                if (columnValue != null && !columnValue.trim().isEmpty()) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
                }
            }
            table.put(put);
            System.out.println(namespace + ":" + tableName + " " + rowKey + " put success");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 从HBase删数据
    public static void deleteRow(Connection connection, String namespace, String tableName, String rowKey) {
        try (Table table = connection.getTable(TableName.valueOf(namespace, tableName))) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println(namespace + ":" + tableName + " " + rowKey + " delete success");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 从HBase查询数据
    public static <T> T getRow(Connection connection, String namespace, String tableName, String rowKey, Class<T> clazz, boolean... isUnderLintToCamel) {
        boolean b = isUnderLintToCamel.length > 0 && isUnderLintToCamel[0];
        TableName tableNameObject = TableName.valueOf(namespace, tableName);
        try (Table table = connection.getTable(tableNameObject)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            if (cells != null && !cells.isEmpty()) {
                T o = clazz.newInstance();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (b) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(o, columnName, columnValue);
                }
                return o;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    // 获取异步操作HBase连接对象
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZK_QUORUM);
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 关闭异步操作HBase连接对象
    public static void closeHBaseAsyncConnection(AsyncConnection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // 异步从HBase读取数据
    public static JSONObject readDimAsync(AsyncConnection connection, String namespace, String tableName, String rowKey) {
        TableName table = TableName.valueOf(namespace, tableName);
        AsyncTable<AdvancedScanResultConsumer> asyncTable = connection.getTable(table);
        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if (cells != null && !cells.isEmpty()) {
                JSONObject jsonObject = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObject.put(columnName, columnValue);
                }
                return jsonObject;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
