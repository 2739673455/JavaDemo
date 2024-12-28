//剪切文件夹
package com.atguigu.fileoperator;

import java.io.*;

public class Homework9 {
    public static void main(String[] args) throws IOException {
        File srcFile = new File("d:\\001");
        File destFile = new File("d:\\002");
        moveFile(srcFile, destFile);
    }

    public static void moveFile(File srcFile, File destFile) throws IOException {
        copyFile(srcFile, destFile);
        delFile(srcFile);
    }

    public static void copyFile(File srcFile, File destFile) throws IOException {
        if (srcFile.isDirectory()) {
            destFile.mkdirs();
            for (File file : srcFile.listFiles()) {
                copyFile(file, new File(destFile, file.getName()));
            }
        } else {
            copyOne(srcFile, destFile);
        }
    }

    public static void copyOne(File srcFile, File destFile) throws IOException {
        if (srcFile.isFile()) {
            try (FileInputStream fis = new FileInputStream(srcFile);
                 FileOutputStream fos = new FileOutputStream(destFile);
                 BufferedInputStream bis = new BufferedInputStream(fis);
                 BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                byte[] buffer = new byte[1024];
                int length = buffer.length;
                while ((length = bis.read(buffer)) != -1) {
                    bos.write(buffer, 0, length);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void delFile(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                delFile(f);
            }
        }
        file.delete();
    }
}
