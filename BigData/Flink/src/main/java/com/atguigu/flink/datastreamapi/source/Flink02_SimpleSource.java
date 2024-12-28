package com.atguigu.flink.datastreamapi.source;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink02_SimpleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //readFromCollection(env);
        //readFromSocket(env);
        //readFromFile(env);

        env.execute();
    }

    private static void readFromFile(StreamExecutionEnvironment env) {
        //3. 从文件读取数据
        env.readTextFile("input/word.txt")
                .map(line -> {
                    String[] split = line.split(",");
                    return new Event(split[0], split[1], Long.parseLong(split[2]));
                })
                .print("from file ");
    }

    private static void readFromSocket(StreamExecutionEnvironment env) {
        //2. 从端口读取数据
        env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Event(split[0], split[1], Long.parseLong(split[2]));
                })
                .print("from socket ");
    }

    public static void readFromCollection(StreamExecutionEnvironment env) {
        //1. 从集合中读取数据
        List<Event> eventList = Arrays.asList(
                new Event("zhangsan", "/home", 1000L),
                new Event("lisi", "/home", 2000L),
                new Event("wangwu", "/home", 3000L),
                new Event("zhangliu", "/home", 4000L)
        );
        env.fromCollection(eventList).print("from collection ");

        env.fromElements(
                new Event("zhangsan", "/home", 1000L),
                new Event("lisi", "/home", 2000L),
                new Event("wangwu", "/home", 3000L),
                new Event("zhangliu", "/home", 4000L)
        ).print("from elements ");
    }
}
