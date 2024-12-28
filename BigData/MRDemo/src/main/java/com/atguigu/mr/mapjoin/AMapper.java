package com.atguigu.mr.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class AMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    HashMap<String, String> map = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        context.write(new Text(line[0] + "\t" + map.get(line[1]) + "\t" + line[2]), NullWritable.get());
    }

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
        try (FileSystem fs = FileSystem.get(context.getConfiguration());
             FSDataInputStream fis = fs.open(new Path(context.getCacheFiles()[0]));
             BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] lines = line.split("\t");
                map.put(lines[0], lines[1]);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
