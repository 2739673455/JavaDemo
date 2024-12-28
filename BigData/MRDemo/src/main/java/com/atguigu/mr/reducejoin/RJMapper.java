package com.atguigu.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class RJMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private String fileName;

    @Override
    protected void setup(Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context) {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        if ("amount.txt".equals(fileName)) {
            context.write(new OrderBean(line[0], line[1], "", Long.parseLong(line[2])), NullWritable.get());
        } else if ("pd.txt".equals(fileName)) {
            context.write(new OrderBean("", line[0], line[1], 1), NullWritable.get());
        }
    }
}
