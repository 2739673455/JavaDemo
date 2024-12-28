package com.atguigu.flink.datastreamapi.transformation;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/*
connect合流: 将不同数据类型的两条流合并
* */

public class Flink10_ConnectStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(3), WatermarkStrategy.noWatermarks(), "ds");
        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);

        ConnectedStreams<Event, Integer> connectStream = ds.connect(intStream);
        //SingleOutputStreamOperator<String> connectDS = connectStream.map(
        //        new CoMapFunction<Event, Integer, String>() {
        //            @Override
        //            public String map1(Event event) {
        //                return event.getTimestamp().toString();
        //            }
        //
        //            @Override
        //            public String map2(Integer integer) {
        //                return integer.toString();
        //            }
        //        }
        //);
        SingleOutputStreamOperator<String> connectDS = connectStream.process(
                new CoProcessFunction<Event, Integer, String>() {
                    @Override
                    public void processElement1(Event o, CoProcessFunction.Context context, Collector collector) {
                        collector.collect(o.toString());
                    }

                    @Override
                    public void processElement2(Integer o, CoProcessFunction.Context context, Collector collector) {
                        collector.collect(o.toString());
                    }
                }
        );
        connectDS.print();

        env.execute();
    }
}
