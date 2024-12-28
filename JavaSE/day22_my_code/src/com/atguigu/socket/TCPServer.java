package com.atguigu.socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServer {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(8888);

        Socket socket = serverSocket.accept();
        System.out.println(socket.getInetAddress());

        InputStream is = socket.getInputStream();
        byte[] data = new byte[1024];
        while (true) {
            int len = is.read(data);
            if (len == -1) break;
            System.out.println(new String(data, 0, len));
        }

        OutputStream os = socket.getOutputStream();
        os.write("hi".getBytes());

        os.close();
        is.close();
        socket.close();
        serverSocket.close();
    }
}
