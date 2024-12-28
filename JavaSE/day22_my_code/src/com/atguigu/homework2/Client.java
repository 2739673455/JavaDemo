package com.atguigu.homework2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 8888);
        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.print("请输入:");
            String message = input.nextLine();
            if ("stop".equals(message)) {
                break;
            }
            sendMessage(socket, message);
//            receiveMessage(socket);
        }
        input.close();
        socket.close();
    }

    public static void receiveMessage(Socket socket) throws IOException {
        InputStream is = socket.getInputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = is.read(buffer)) != -1) {
            System.out.println(is.read(buffer, 0, len));
        }
        is.close();
    }

    public static void sendMessage(Socket socket, String message) throws IOException {
        OutputStream os = socket.getOutputStream();
        os.write(message.getBytes());
        os.close();
        System.out.println(message + " 已发送" + ":" + System.currentTimeMillis());
    }
}
