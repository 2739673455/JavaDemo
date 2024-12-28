package com.atguigu.flink.datastreamapi.transformation;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class Flink08_SideOutputSplitStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

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

        mainDS.print("main ");
        mainDS.getSideOutput(zhangsanTag).print("zhangsan ");
        mainDS.getSideOutput(lisiTag).print("lisi ");

        env.execute();
    }
}
