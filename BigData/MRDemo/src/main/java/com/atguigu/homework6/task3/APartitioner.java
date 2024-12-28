package com.atguigu.homework6.task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class APartitioner extends Partitioner<Amount, Text> {
    @Override
    public int getPartition(Amount user, Text order_id, int i) {
        switch (user.getUser_id()) {
            case "0":
                return 0;
            case "1":
                return 1;
            case "2":
                return 2;
            case "3":
                return 3;
            case "4":
                return 4;
            case "5":
                return 5;
            case "6":
                return 6;
            case "7":
                return 7;
            case "8":
                return 8;
            default:
                return 9;
        }
    }
}
