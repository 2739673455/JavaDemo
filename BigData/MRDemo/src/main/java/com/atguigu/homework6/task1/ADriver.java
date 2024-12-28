package com.atguigu.homework6.task1;

import com.atguigu.tools.PrintResult;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ADriver {
    private static String dirName;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        boolean b = step1();
        if (b) {
            step2();
        }
    }

    public static boolean step1() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(ADriver.class);
        job.setMapperClass(AMapper.class);
        job.setReducerClass(AReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\mrtest\\friends.txt"));
        dirName = "D:\\mrtest\\output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(dirName));
        boolean b = job.waitForCompletion(true);
        System.out.println("job finished : " + b);
        return b;
    }

    public static void step2() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(ADriver.class);
        job.setMapperClass(BMapper.class);
        job.setReducerClass(BReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(dirName + "\\part-r-00000"));
        dirName = "D:\\mrtest\\output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(dirName));
        boolean b = job.waitForCompletion(true);
        System.out.println("job finished : " + b);

        PrintResult.run(b, dirName);
    }
}
