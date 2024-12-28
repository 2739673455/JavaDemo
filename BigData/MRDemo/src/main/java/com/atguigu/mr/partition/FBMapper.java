package com.atguigu.mr.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FBMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String[] info = value.toString().split("\t");
        long upFlow = Long.parseLong(info[info.length - 3]);
        long downFlow = Long.parseLong(info[info.length - 2]);
        FlowBean flowBean = new FlowBean(upFlow, downFlow, upFlow + downFlow);
        context.write(new Text(info[1]), flowBean);
    }
}
