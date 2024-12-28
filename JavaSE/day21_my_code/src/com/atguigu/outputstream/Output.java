package com.atguigu.outputstream;

import java.io.*;
import java.util.Scanner;

public class Output {
    public static void main(String[] args) throws IOException {
//        test1();
//        test2();
//        test3();
//        test4();
        test5();
    }

    public static void test1() throws IOException {
        Scanner input = new Scanner(System.in);
        FileWriter fw = new FileWriter("output.txt");
        String content = "";
        while (!"stop".equals(content)) {
            content = input.next();
            fw.write(content + "\n");
        }
        input.close();
        fw.close();
    }

    public static void test2() throws IOException {
        Scanner input = new Scanner(System.in);
        PrintStream ps = new PrintStream("output.txt");
        String content = "";
        while (!"stop".equals(content)) {
            content = input.next();
            ps.println(content);
        }
        input.close();
        ps.close();
    }

    public static void test3() throws IOException {
        Scanner input = new Scanner(System.in);
        FileWriter fw = new FileWriter("output.txt");
        BufferedWriter bw = new BufferedWriter(fw);
        String content = "";
        while (!"stop".equals(content)) {
            content = input.next();
            bw.write(content);
            bw.newLine();
        }
        input.close();
        bw.close();
    }

    public static void test4() throws IOException {
        File file = new File("D:\\Code\\JavaProject\\20240522java\\JavaSE_Code\\day21_my_code\\src\\com\\atguigu\\outputstream\\Output.java");
        Scanner input = new Scanner(file);
        while (input.hasNext()) {
            System.out.println(input.next());
        }
        input.close();
    }

    public static void test5() throws IOException {
        File file = new File("D:\\Code\\JavaProject\\20240522java\\JavaSE_Code\\day21_my_code\\src\\com\\atguigu\\outputstream\\Output.java");
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        while (true) {
            String line = br.readLine();
            if (line == null) break;
            System.out.println(line);
        }
        br.close();
        fr.close();
    }
}
