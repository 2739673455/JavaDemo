package com.atguigu.homework5.task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> fileCount = new HashMap<>();
        for (Text fileName : values) {
            fileCount.put(fileName.toString(), fileCount.getOrDefault(fileName.toString(), 0) + 1);
        }

        List<Map.Entry<String, Integer>> list = new ArrayList<>(fileCount.entrySet());
        list.sort((o1, o2) -> -(o1.getValue() - o2.getValue()));
        String result = "";
        for (Map.Entry<String, Integer> item : list) {
            result += item.getKey() + ":" + item.getValue() + ",";
        }
        result = result.substring(0, result.length() - 1);
        context.write(key, new Text(result));
    }
}
