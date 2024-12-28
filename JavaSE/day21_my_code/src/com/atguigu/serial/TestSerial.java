package com.atguigu.serial;

import java.io.*;

public class TestSerial {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String filename = "序列化文件.🐕🐕🐕";
//        int i1 = 1;
//        double d1 = 1.0;
//        char c1 = 'a';
//        boolean b1 = true;
//        Student s1 = new Student("你", 1);
//
//        FileOutputStream fos = new FileOutputStream(filename);
//        ObjectOutputStream oos = new ObjectOutputStream(fos);
//
//        oos.writeInt(i1);
//        oos.writeDouble(d1);
//        oos.writeChar(c1);
//        oos.writeBoolean(b1);
//        oos.writeObject(s1);
//
//        oos.close();
//        fos.close();


        FileInputStream fis = new FileInputStream(filename);
        ObjectInputStream ois = new ObjectInputStream(fis);

        int i2 = ois.readInt();
        double d2 = ois.readDouble();
        char c2 = ois.readChar();
        boolean b2 = ois.readBoolean();
        Object o2 = ois.readObject();

        System.out.println(i2);
        System.out.println(d2);
        System.out.println(c2);
        System.out.println(b2);
        System.out.println(o2);

        fis.close();
        ois.close();
    }
}
