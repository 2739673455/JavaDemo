package com.atguigu.socket;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

public class TCPClient {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("192.168.33.64", 8888);

        sendFile(socket);

        socket.close();
    }

    public static void sendFile(Socket socket) throws IOException {
        File file = new File("C:\\Users\\27396\\Pictures\\wallhaven-p89ex9.jpg");
        String fileName = file.getName();
        OutputStream os = socket.getOutputStream();
        FileInputStream fis = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fis);

        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeUTF(fileName);

        byte[] bytes = new byte[1024];
        while (true) {
            int len = bis.read(bytes);
            if (len == -1) break;
            oos.write(bytes, 0, len);
            oos.flush();
        }
        socket.shutdownInput();

        receiveMessage(socket);

        oos.close();
        bis.close();
        fis.close();
        os.close();
    }

    public static void receiveFile(Socket socket) throws IOException {
        InputStream is = socket.getInputStream();
        FileOutputStream fos = new FileOutputStream("1.jpg");
        byte[] data = new byte[1024];
        while (true) {
            int len = is.read(data);
            if (len == -1) break;
            fos.write(data, 0, len);
        }
        fos.close();
        is.close();
    }

    public static void receiveMessage(Socket socket) throws IOException {
        InputStream is = socket.getInputStream();
        Scanner sc = new Scanner(is);
        while (sc.hasNextLine()) {
            System.out.println(sc.nextLine());
        }
        sc.close();
        is.close();
    }
}
