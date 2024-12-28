package com.atguigu.homework6.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split1 = value.toString().split(":");
        String left = split1[0];
        String[] rights = split1[1].split(",");
        for (String right : rights) {
            System.out.println(right + " " + left);
            context.write(new Text(right), new Text(left));
        }
    }
}
