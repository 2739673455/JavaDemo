package com.atguigu.flink.datastreamapi.transformation;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink05_UserDefinePartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(7), WatermarkStrategy.noWatermarks(), "ds");
        ds.partitionCustom(
                new Partitioner<String>() {
                    @Override
                    public int partition(String s, int i) {
                        return Math.abs(s.hashCode() % 5);
                    }
                },
                Event::getUser
        ).print();

        env.execute();
    }
}
