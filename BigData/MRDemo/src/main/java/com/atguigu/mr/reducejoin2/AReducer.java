package com.atguigu.mr.reducejoin2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class AReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String pname = "";
        StringBuilder result = new StringBuilder();
        ArrayList<String[]> lines = new ArrayList<>();

        System.out.println(key.toString());

        for (NullWritable value : values) {
            String[] line = key.toString().split("\t");
            if (!" ".equals(line[3])) {
                pname = line[3];
            } else if (!"".equals(pname)) {
                returnContext(line, pname, result, context);
            } else {
                lines.add(line);
            }
        }
        for (String[] line : lines) {
            returnContext(line, pname, result, context);
        }
    }

    public void returnContext(String[] line, String pname, StringBuilder result, Context context) throws IOException, InterruptedException {
        System.out.println(Arrays.toString(line) + pname);
        line[0] = line[1];
        line[1] = pname;
        result = new StringBuilder();
        for (String s : line) {
            result.append(s).append("\t");
        }
        context.write(new Text(result.substring(0, result.length() - 3)), NullWritable.get());
    }
}
