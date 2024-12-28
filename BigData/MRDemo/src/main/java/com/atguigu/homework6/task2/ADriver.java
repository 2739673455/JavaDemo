package com.atguigu.homework6.task2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ADriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(ADriver.class);
        job.setMapperClass(AMapper.class);
        job.setReducerClass(AReducer.class);
        job.setMapOutputKeyClass(Amount.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Amount.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\mrtest\\order.csv"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\mrtest\\output" + System.currentTimeMillis()));
        boolean b = job.waitForCompletion(true);
        System.out.println("job finished : " + b);
    }
}
