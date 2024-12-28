package com.atguigu.homework5.task3;

import com.atguigu.tools.PrintResult;
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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\mrtest\\input1"));
        String dirName = "D:\\mrtest\\output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(dirName));
        boolean b = job.waitForCompletion(true);

        PrintResult.run(b, dirName);
    }
}
