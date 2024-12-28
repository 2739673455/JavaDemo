package com.atguigu.socket;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class UDPReceive {
    public static void main(String[] args) throws Exception {
        DatagramSocket ds = new DatagramSocket(8888);//双方端口号必须对应上
        byte[] bytes = new byte[1024];
        DatagramPacket dp = new DatagramPacket(bytes, bytes.length);
        ds.receive(dp);
        System.out.println(new String(bytes, 0, dp.getLength()));
        ds.close();
    }
}
