package com.atguigu.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ConnectionUtil {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        //同步的connection

        //Connection connection = ConnectionFactory.createConnection();
        //
        //System.out.println(connection);
        //
        //connection.close();

        //异步的connection


        //CompletableFuture<AsyncConnection> future = ConnectionFactory.createAsyncConnection();
        //AsyncConnection asyncConnection = future.get();
        //
        //System.out.println(asyncConnection);
        //
        //asyncConnection.close();
    }


    public static Connection getConnection() {
        try {
            return ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeConnection(Connection connection) {
        try {
            if (connection != null) connection.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}