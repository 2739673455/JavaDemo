package com.atguigu.mr.reducejoin2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AGrouping extends WritableComparator {
    public AGrouping() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String pid1 = a.toString().split("\t")[0];
        String pid2 = b.toString().split("\t")[0];
        return pid1.compareTo(pid2);
    }
}