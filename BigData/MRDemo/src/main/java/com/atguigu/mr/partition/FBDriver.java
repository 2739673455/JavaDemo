package com.atguigu.mr.partition;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FBDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();

        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(5);

        job.setJarByClass(FBDriver.class);
        job.setMapperClass(FBMapper.class);
        job.setReducerClass(FBReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\mrtest\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\mrtest\\output" + System.currentTimeMillis()));
        boolean b = job.waitForCompletion(true);
        System.out.println("job finished : " + b);
    }
}
