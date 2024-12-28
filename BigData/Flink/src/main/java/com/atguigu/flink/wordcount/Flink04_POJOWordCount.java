package com.atguigu.flink.wordcount;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class Flink04_POJOWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        streamSource
                .flatMap((String line, Collector<WordCount> collector) -> Arrays.stream(line.split(" ")).forEach(word -> collector.collect(new WordCount(word, 1L))))
                .returns(Types.POJO(WordCount.class))
                .keyBy(WordCount::getWord)
                .sum("count")
                .print();
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class WordCount {
        private String word;
        private Long count;

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
