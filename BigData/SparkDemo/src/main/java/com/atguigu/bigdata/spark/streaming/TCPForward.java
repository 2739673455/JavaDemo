package com.atguigu.bigdata.spark.streaming;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class TCPForward {
    public static void main(String[] args) {
        int port = 9999; // 监听的端口号
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("TCP 服务器已启动，监听端口：" + port);

            // 等待客户端连接（这里即为 Spark Streaming 程序）
            Socket clientSocket = serverSocket.accept();
            System.out.println("客户端已连接：" + clientSocket.getInetAddress());

            // 获取输出流，向客户端发送数据
            OutputStream outputStream = clientSocket.getOutputStream();
            PrintWriter writer = new PrintWriter(outputStream, true);

            // 使用控制台输入模拟数据发送
            Scanner scanner = new Scanner(System.in);
            System.out.println("请输入要发送的数据，输入 'exit' 退出：");

            String input;
            while (!(input = scanner.nextLine()).equalsIgnoreCase("exit")) {
                writer.println(input); // 将输入的数据发送给客户端（即 Spark 程序）
            }

            System.out.println("服务器关闭连接。");
            clientSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
