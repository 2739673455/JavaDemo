package com.atguigu.mr.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


//自定义分组方式
public class MyGrouping extends WritableComparator {
    public MyGrouping() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;
        return o1.getPid().compareTo(o2.getPid());
    }
}
