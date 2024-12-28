package com.atguigu.close;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class TestClose {
    public static void main(String[] args) {
        try (
                FileInputStream fis = new FileInputStream("D:\\Code\\JavaProject\\20240522java\\JavaSE_Code\\day22_my_code\\src\\com\\atguigu\\close\\TestClose.java");
                BufferedInputStream bis = new BufferedInputStream(fis);
                InputStreamReader isr = new InputStreamReader(bis);
                BufferedReader br = new BufferedReader(isr);
        ) {
            char[] buf = new char[1024];
            int len = buf.length;
            while (len == buf.length) {
                len = br.read(buf);
                System.out.print(new String(buf, 0, len));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
