package com.atguigu.bigdata.spark.streaming;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class TCPClient {
    public static void main(String[] args) throws IOException, InterruptedException {
        while (true) {
            try {
                Socket socket = new Socket("192.168.33.34", 9999);
                OutputStream os = socket.getOutputStream();
                os.write("whoami".getBytes());
                os.close();
                socket.close();
                System.out.println("Sended" + System.currentTimeMillis());
            } catch (Exception e) {
                System.out.println("Connection timed out: connect " + System.currentTimeMillis());
            }
        }
    }
}