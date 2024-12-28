package com.atguigu.mr.reducejoin;

import com.atguigu.tools.PrintResult;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RJDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(RJDriver.class);
        job.setMapperClass(RJMapper.class);
        job.setReducerClass(RJReducer.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(MyGrouping.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\mrtest\\joininput"));
        String dirName = "D:\\mrtest\\output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(dirName));
        boolean b = job.waitForCompletion(true);
        System.out.println("job finished : " + b);

        PrintResult.run(b, dirName);

    }
}
