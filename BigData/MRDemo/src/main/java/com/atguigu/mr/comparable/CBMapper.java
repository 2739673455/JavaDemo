package com.atguigu.mr.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CBMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        String[] info = value.toString().split("\t");
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Long.parseLong(info[1]));
        flowBean.setDownFlow(Long.parseLong(info[2]));
        flowBean.setSumFlow(Long.parseLong(info[3]));
        context.write(flowBean, new Text(info[0]));
    }
}
