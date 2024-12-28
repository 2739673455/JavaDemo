package com.atguigu.mr.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean fl, Text text, int i) {
        if (text.toString().startsWith("136")) {
            return 0;
        } else if (text.toString().startsWith("137")) {
            return 1;
        } else if (text.toString().startsWith("138")) {
            return 2;
        } else if (text.toString().startsWith("139")) {
            return 3;
        } else {
            return 4;
        }
    }
}
