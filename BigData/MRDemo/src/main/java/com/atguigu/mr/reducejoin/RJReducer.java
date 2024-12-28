package com.atguigu.mr.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RJReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String pname = key.getPname();
        for (NullWritable value : values) {
            if (!"".equals(key.getId())) {
                key.setPname(pname);
                context.write(key, NullWritable.get());
            }
        }
    }
}
