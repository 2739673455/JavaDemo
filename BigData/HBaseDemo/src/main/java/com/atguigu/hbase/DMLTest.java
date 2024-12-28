package com.atguigu.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class DMLTest {
    public static void main(String[] args) throws Exception {
        Connection connection = ConnectionUtil.getConnection();

        //putData(connection, null, "t1", "1001", "f1", "name", "zhangsan");
        //deleteData(connection, null, "t1", "1002", "f1", "name");
        //getData(connection, null, "t1", "1001", "f1", "name");
        //scanData(connection, null, "t1", "1001", "1002!");
        scanFilterData(connection, null, "t1", "1001", "1002!");

        ConnectionUtil.closeConnection(connection);
    }

    public static void putData(Connection connection, String nameSpace, String tableName, String rowKey, String cf, String col, String value) throws IOException {
        TableName tn = TableName.valueOf(nameSpace, tableName);
        Table table = connection.getTable(tn);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
        table.put(put);

        table.close();
    }

    public static void deleteData(Connection connection, String nameSpace, String tableName, String rowKey, String cf, String col) throws IOException {
        TableName tn = TableName.valueOf(nameSpace, tableName);
        Table table = connection.getTable(tn);

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //delete
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col));

        //deleteColumn
        //delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(col));

        //deleteFamily
        //delete.addFamily(Bytes.toBytes(cf));
        table.delete(delete);

        table.close();
    }

    public static void getData(Connection connection, String nameSpace, String tableName, String rowKey, String cf, String col) throws IOException {
        TableName tn = TableName.valueOf(nameSpace, tableName);
        Table table = connection.getTable(tn);

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(rowKey + "  " + cf + ":" + col + "  " + value);
        }

        table.close();
    }

    public static void scanData(Connection connection, String nameSpace, String tableName, String startRow, String stopRow) throws IOException {
        TableName tn = TableName.valueOf(nameSpace, tableName);
        Table table = connection.getTable(tn);

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(row + "  " + family + ":" + column + "  " + value);
            }
        }

        table.close();
    }

    //scan 非rowKey列过滤
    public static void scanFilterData(Connection connection, String nameSpace, String tableName, String startRow, String stopRow) throws IOException {
        TableName tn = TableName.valueOf(nameSpace, tableName);
        Table table = connection.getTable(tn);

        Scan scan = new Scan();
        SingleColumnValueFilter ageFilter = new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("age"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("25"));
        ageFilter.setFilterIfMissing(true); //col若不存在则跳过
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("name"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("lisi"));
        nameFilter.setFilterIfMissing(true); //col若不存在则跳过
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, ageFilter, nameFilter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(row + "  " + family + ":" + column + "  " + value);
            }
        }

        table.close();
    }
}