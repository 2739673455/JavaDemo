package com.atguigu.mr.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean o2, int i) {
        String phoneNumber = text.toString();
        if (phoneNumber.startsWith("134")) {
            return 0;
        } else if (phoneNumber.startsWith("135")) {
            return 1;
        } else if (phoneNumber.startsWith("136")) {
            return 2;
        } else if (phoneNumber.startsWith("137")) {
            return 3;
        } else {
            return 4;
        }
    }
}
