package com.atguigu.mr.comparable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CBDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();

        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(5);

        job.setJarByClass(CBDriver.class);
        job.setMapperClass(CBMapper.class);
        job.setReducerClass(CBReducer.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\mrtest\\flowbean.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\mrtest\\output" + System.currentTimeMillis()));
        boolean b = job.waitForCompletion(true);
        System.out.println("job finished : " + b);
    }
}
