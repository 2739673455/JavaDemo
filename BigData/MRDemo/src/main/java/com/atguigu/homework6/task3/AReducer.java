package com.atguigu.homework6.task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AReducer extends Reducer<Amount, Text, Text, Amount> {
    @Override
    protected void reduce(Amount key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), key);
    }
}
