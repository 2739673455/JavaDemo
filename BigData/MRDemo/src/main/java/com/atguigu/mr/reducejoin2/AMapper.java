package com.atguigu.mr.reducejoin2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String result;
        if (line.length == 3) {
            result = line[1] + "\t" + line[0] + "\t" + line[2] + "\t" + " ";
        } else {
            result = line[0] + "\t" + " " + "\t" + " " + "\t" + line[1];
        }
        context.write(new Text(result), NullWritable.get());
    }
}