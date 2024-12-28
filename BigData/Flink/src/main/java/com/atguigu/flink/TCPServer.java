package com.atguigu.flink;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class TCPServer {
    static boolean stop = true;
    static Socket clientSocket = null;

    public static void main(String[] args) throws IOException {
        int port = 9999;
        ServerSocket serverSocket = new ServerSocket(port);
        try {
            LinkedList<String> msgQueue = new LinkedList<>();
            ScannerThread scannerThread = new ScannerThread(msgQueue);
            scannerThread.setName("scannerThread");
            scannerThread.setDaemon(true);
            scannerThread.start();
            while (true) {
                clientSocket = serverSocket.accept();
                System.out.println(clientSocket.getInetAddress() + " connected");
                System.out.print(clientSocket.getInetAddress() + ":" + clientSocket.getPort() + " << ");
                stop = false;

                SendThread sendThread = new SendThread(clientSocket, msgQueue);
                ReceiveThread receiveThread = new ReceiveThread(clientSocket);
                sendThread.setName("SendThread");
                receiveThread.setName("ReceiveThread");
                sendThread.setDaemon(true);
                receiveThread.setDaemon(true);
                sendThread.start();
                receiveThread.start();

                while (!stop) {
                    Thread.sleep(500);
                }
                clientSocket.close();
                clientSocket = null;
                System.out.println("\rconnection close");
            }
        } catch (Exception e) {
            serverSocket.close();
        }
    }

    private static class ScannerThread extends Thread {
        public LinkedList<String> msgQueue;

        public ScannerThread(LinkedList<String> msgQueue) {
            this.msgQueue = msgQueue;
        }

        public void run() {
            while (true) {
                if (stop) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    continue;
                }
                System.out.print("\r" + clientSocket.getInetAddress() + ":" + clientSocket.getPort() + " << ");
                Scanner scanner = new Scanner(System.in);
                String message = scanner.nextLine();
                msgQueue.add(message);
            }
        }
    }

    private static class SendThread extends Thread {
        public Socket clientSocket;
        public LinkedList<String> msgQueue;

        public SendThread(Socket clientSocket, LinkedList<String> msgQueue) {
            this.clientSocket = clientSocket;
            this.msgQueue = msgQueue;
        }

        public void run() {
            try {
                OutputStream outputStream = clientSocket.getOutputStream();
                PrintWriter writer = new PrintWriter(outputStream, true);
                String message = null;
                while (!stop) {
                    try {
                        message = msgQueue.pop();
                    } catch (NoSuchElementException e) {
                        Thread.sleep(500);
                        continue;
                    }
                    writer.println(message);
                }
            } catch (IOException | InterruptedException e) {
                stop = true;
            }
        }
    }

    private static class ReceiveThread extends Thread {
        public Socket clientSocket;

        public ReceiveThread(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        public void run() {
            try {
                InputStream inputStream = clientSocket.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                char[] message = new char[1024];
                while (!stop) {
                    int len = inputStreamReader.read(message);
                    if (len == -1) {
                        stop = true;
                        break;
                    }
                    String strMessage = String.valueOf(message, 0, len);
                    String recvMessage = "\r" + clientSocket.getInetAddress() + ":" + clientSocket.getPort() + " >> " + strMessage;
                    String sendMessage = clientSocket.getInetAddress() + ":" + clientSocket.getPort() + " << ";
                    System.out.print(recvMessage + sendMessage);
                }
            } catch (IOException ignored) {
                stop = true;
            }
        }
    }
}