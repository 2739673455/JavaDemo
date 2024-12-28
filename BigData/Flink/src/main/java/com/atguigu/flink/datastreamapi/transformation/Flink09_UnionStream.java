package com.atguigu.flink.datastreamapi.transformation;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/*
union合流: 合并多条流,要求被合并的流中数据类型一致
* */

public class Flink09_UnionStream {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 5678);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        OutputTag<Event> zhangsanTag = new OutputTag<>("zhangsan->", Types.POJO(Event.class));
        OutputTag<Event> lisiTag = new OutputTag<>("lisi->", Types.POJO(Event.class));

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(3), WatermarkStrategy.noWatermarks(), "ds");
        SingleOutputStreamOperator<Event> mainDS = ds.process(
                new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> out) throws Exception {
                        if ("zhangsan".equals(event.getUser()))
                            context.output(zhangsanTag, event);
                        else if ("lisi".equals(event.getUser()))
                            context.output(lisiTag, event);
                        else
                            out.collect(event);
                    }
                }
        );

        SideOutputDataStream<Event> zhangsanStream = mainDS.getSideOutput(zhangsanTag);
        SideOutputDataStream<Event> lisiStream = mainDS.getSideOutput(lisiTag);

        DataStream<Event> unionDS = mainDS.union(zhangsanStream, lisiStream);
        unionDS.print();

        env.execute();
    }
}
