package com.atguigu.bigdata.spark.core.rdd.instance;

import java.io.*;

public class Display {
    public static void display(String str) throws IOException {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        File path = new File(str);
        File[] files = path.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith("part");
            }
        });
        assert files != null;
        for (File file : files) {
            System.out.println("----- " + file.getName() + " -----");
            fis = new FileInputStream(file);
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);
            while (br.ready()) {
                System.out.println(br.readLine());
            }
        }
        assert br != null;
        br.close();
        isr.close();
        fis.close();
    }
}
