package com.atguigu.homework5.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AReducer extends Reducer<Text, LongWritable, Text, Amount> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long totalAmount = 0;
        long avgAmount = 0;
        long maxAmount = Long.MIN_VALUE;
        long minAmount = Long.MAX_VALUE;
        long count = 0;
        for (LongWritable value : values) {
            ++count;
            totalAmount += value.get();
            maxAmount = Math.max(maxAmount, value.get());
            minAmount = Math.min(minAmount, value.get());
        }
        avgAmount = totalAmount / count;
        Amount amount = new Amount(totalAmount, avgAmount, maxAmount, minAmount);
        context.write(key, amount);
    }
}
