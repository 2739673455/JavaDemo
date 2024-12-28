package com.atguigu.homework2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class Server {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(8888);
        Socket socket = serverSocket.accept();
        InputStream is = socket.getInputStream();
        Scanner input = new Scanner(is);
        String message = "";
        while (true) {
            while (input.hasNextLine()) {
                message = input.nextLine();
            }
            if ("stop".equals(message)) {
                System.out.println("链接已断开");
                break;
            }
            System.out.println(message);

//            if (!receiveMessage(socket)) {
//                System.out.println("链接已断开");
//                break;
//            }
//            String message = "1";
//            sendMessage(socket, message);

        }
        input.close();
        is.close();
        socket.close();
        serverSocket.close();
    }

    public static boolean receiveMessage(Socket socket) throws IOException {
        InputStream is = socket.getInputStream();
        byte[] buffer = new byte[1024];
        int len;
        String message = "";
        while ((len = is.read(buffer)) != -1) {
            message += new String(buffer, 0, len);
        }
        is.close();
        if ("stop".equals(message)) {
            return false;
        } else {
            System.out.println(System.currentTimeMillis() + ":" + message);
            return true;
        }
    }

    public static void sendMessage(Socket socket, String message) throws IOException {
        OutputStream os = socket.getOutputStream();
        os.write(message.getBytes());
        os.close();
    }
}
