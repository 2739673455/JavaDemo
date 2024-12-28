package com.atguigu.socket;

import java.io.IOException;
import java.net.*;

public class UDPSend {
    public static void main(String[] args) throws IOException {
        while (true) {
            DatagramSocket socket = new DatagramSocket();
            byte[] bytes = "whoami".getBytes();
            byte[] ipAddress = {(byte) 192, (byte) 168, 33, (byte) 255};
            InetAddress ip = InetAddress.getByAddress(ipAddress);
            DatagramPacket packet = new DatagramPacket(bytes, bytes.length, ip, 9999);
            socket.send(packet);
            socket.close();
        }
    }
}
