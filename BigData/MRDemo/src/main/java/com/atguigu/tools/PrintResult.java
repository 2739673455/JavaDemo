package com.atguigu.tools;

import java.io.*;

public class PrintResult {
    public static void run(boolean b, String dirName) throws IOException {
        System.out.println("========== " + b + " ==========");
        FileInputStream fis = null;
        BufferedReader br = null;
        if (b) {
            try {
                fis = new FileInputStream(new File(dirName + "\\part-m-00000"));
            } catch (FileNotFoundException e) {
                fis = new FileInputStream(new File(dirName + "\\part-r-00000"));
            }
            br = new BufferedReader(new InputStreamReader(fis));
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            br.close();
            fis.close();
        }
    }
}
