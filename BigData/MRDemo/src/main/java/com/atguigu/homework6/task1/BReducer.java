package com.atguigu.homework6.task1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String lefts = "";
        for (Text left : values) {
            lefts = lefts + left.toString() + ",";
        }
        lefts = lefts.substring(0, lefts.length() - 1);

        context.write(new Text(key.toString() + lefts), NullWritable.get());
    }
}
