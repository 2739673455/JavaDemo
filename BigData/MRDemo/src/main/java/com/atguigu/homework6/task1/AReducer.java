package com.atguigu.homework6.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class AReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        ArrayList<String> lefts = new ArrayList<>();
        for (Text value : values) {
            if (lefts.isEmpty()) {
                lefts.add(value.toString());
            } else {
                for (String left : lefts) {
                    String result;
                    if (left.compareTo(value.toString()) < 0) {
                        result = left + "-" + value + ":";
                    } else {
                        result = value + "-" + left + ":";
                    }
                    context.write(new Text(result), key);
                }
                lefts.add(value.toString());
            }
        }
    }
}
