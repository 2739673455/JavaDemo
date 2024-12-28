package com.atguigu.mr.mapjoin;

import com.atguigu.tools.PrintResult;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class ADriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Job job = Job.getInstance();
        job.setJarByClass(ADriver.class);
        job.setMapperClass(AMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        job.addCacheFile(new URI("file:///D:/mrtest/joininput/pd.txt"));
        FileInputFormat.setInputPaths(job, new Path("D:\\mrtest\\joininput\\amount.txt"));
        String dirName = "D:\\mrtest\\output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(dirName));
        boolean b = job.waitForCompletion(true);
        PrintResult.run(b, dirName);
    }
}
