package com.atguigu.hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class DDLTest {
    public static void main(String[] args) throws Exception {
        Connection connection = ConnectionUtil.getConnection();

        createNameSpace(connection, "db3");
        deleteNamespace(connection, "db3");
        listNamespace(connection);
        createTable(connection, null, "t2", "f1", "f2");
        deleteTable(connection, null, "t2");
        listTable(connection);
        describeTable(connection, null, "t1");
        HashMap<String, String> conf = new HashMap<>();
        conf.put("VERSIONS", "3");
        modifyTable(connection, null, "t1", "f1", conf);

        ConnectionUtil.closeConnection(connection);
    }

    public static void createNameSpace(Connection connection, String nameSpace) throws Exception {
        if (nameSpace == null || nameSpace.trim().isEmpty()) {
            System.out.println("NameSpace is null or empty");
            return;
        }
        Admin admin = connection.getAdmin();
        String[] nameSpaces = admin.listNamespaces();
        List<String> nameSpaceList = Arrays.asList(nameSpaces);
        if (nameSpaceList.contains(nameSpace)) {
            System.out.println("NameSpace already exist");
            return;
        }
        NamespaceDescriptor.Builder nameSpaceBuilder = NamespaceDescriptor.create(nameSpace);
        NamespaceDescriptor namespaceDescriptor = nameSpaceBuilder.build();
        admin.createNamespace(namespaceDescriptor);
        System.out.println(nameSpace + " created");
        admin.close();
    }

    public static void deleteNamespace(Connection connection, String nameSpace) throws Exception {
        Admin admin = connection.getAdmin();
        String[] nameSpaces = admin.listNamespaces();
        List<String> nameSpaceList = Arrays.asList(nameSpaces);
        if (!nameSpaceList.contains(nameSpace)) {
            System.out.println("NameSpace not exist");
            return;
        }
        admin.deleteNamespace(nameSpace);
        System.out.println(nameSpace + " deleted");
        admin.close();
    }

    public static void listNamespace(Connection connection) throws Exception {
        Admin admin = connection.getAdmin();
        String[] nameSpaces = admin.listNamespaces();
        List<String> nameSpaceList = Arrays.asList(nameSpaces);
        System.out.println(nameSpaceList);
        admin.close();
    }

    public static void createTable(Connection connection, String nameSpace, String tableName, String... cfs) throws IOException {
        if (nameSpace == null || nameSpace.trim().isEmpty())
            nameSpace = "default";
        if (tableName == null || tableName.trim().isEmpty()) {
            System.out.println("TableName is null or empty");
            return;
        }
        if (cfs == null || cfs.length == 0) {
            System.out.println("ColumnFamilys is null or empty");
            return;
        }
        //创建
        Admin admin = connection.getAdmin();
        //  判断表是否存在
        TableName tn = TableName.valueOf(nameSpace, tableName);
        if (admin.tableExists(tn)) {
            System.out.println(nameSpace + ":" + tableName + " already exist");
            return;
        }
        //  获取tableDescriptorBuilder对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn);
        //  设置列族信息
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        admin.createTable(tableDescriptor);
        System.out.println(nameSpace + ":" + tableName + " created");
        admin.close();
    }

    public static void deleteTable(Connection connection, String nameSpace, String tableName) throws IOException {
        if (nameSpace == null || nameSpace.trim().isEmpty())
            nameSpace = "default";
        if (tableName == null || tableName.trim().isEmpty()) {
            System.out.println("TableName is null or empty");
            return;
        }
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(nameSpace, tableName);
        if (!admin.tableExists(tn)) {
            System.out.println(nameSpace + ":" + tableName + " not exist");
            return;
        }
        admin.disableTable(tn);
        admin.deleteTable(tn);
        System.out.println(nameSpace + ":" + tableName + " deleted");
        admin.close();
    }

    public static void listTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableName[] tableNames = admin.listTableNames();
        List<TableName> tableNameList = Arrays.asList(tableNames);
        System.out.println(tableNameList);
        admin.close();
    }

    public static void describeTable(Connection connection, String nameSpace, String tableName) throws IOException {
        if (nameSpace == null || nameSpace.trim().isEmpty())
            nameSpace = "default";
        if (tableName == null || tableName.trim().isEmpty()) {
            System.out.println("TableName is null or empty");
            return;
        }
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(nameSpace, tableName);
        if (!admin.tableExists(tn)) {
            System.out.println(nameSpace + ":" + tableName + " not exist");
            return;
        }
        TableDescriptor descriptor = admin.getDescriptor(tn);
        System.out.println("TableName => " + descriptor.getTableName());
        for (ColumnFamilyDescriptor columnFamily : descriptor.getColumnFamilies()) {
            System.out.println(columnFamily);
        }
        admin.close();
    }

    public static void modifyTable(Connection connection, String nameSpace, String tableName, String cf, Map<String, String> conf) throws IOException {
        if (nameSpace == null || nameSpace.trim().isEmpty())
            nameSpace = "default";
        if (tableName == null || tableName.trim().isEmpty()) {
            System.out.println("TableName is null or empty");
            return;
        }
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(nameSpace, tableName);
        if (!admin.tableExists(tn)) {
            System.out.println(nameSpace + ":" + tableName + " not exist");
            return;
        }
        TableDescriptor tableDescriptor = admin.getDescriptor(tn);
        //判断要修改的cf是否存在
        ColumnFamilyDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        List<String> cfList = Arrays.stream(columnFamilies).map(ColumnFamilyDescriptor::getNameAsString).collect(Collectors.toList());
        if (!cfList.contains(cf)) {
            //若不存在，添加cf
            System.out.println(nameSpace + ":" + tableName + " " + cf + " not exist, will be add");
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            for (String key : conf.keySet()) {
                columnFamilyDescriptorBuilder.setValue(key, conf.get(key));
            }
            ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
            admin.addColumnFamily(tn, columnFamilyDescriptor);
            System.out.println(columnFamilyDescriptor.getValues());
            System.out.println(nameSpace + ":" + tableName + " " + cf + " is added");
        } else {
            //若存在，修改cf或删除
            //删除
            Collection<String> values = conf.values();
            for (String value : values) {
                if ("delete".equals(value)) {
                    admin.deleteColumnFamily(tn, Bytes.toBytes(cf));
                    System.out.println(nameSpace + ":" + tableName + " " + cf + " is deleted");
                    return;
                }
            }
            //修改
            ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor columnFamily = (ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor) tableDescriptor.getColumnFamily(Bytes.toBytes(cf));
            System.out.println(columnFamily.getValues());
            for (String key : conf.keySet()) {
                columnFamily.setValue(key, conf.get(key));
            }
            admin.modifyColumnFamily(tn, columnFamily);
            System.out.println(columnFamily.getValues());
            System.out.println(nameSpace + ":" + tableName + " " + cf + " is modified");
        }
        admin.close();
    }
}