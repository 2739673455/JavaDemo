package com.atguigu.flink.timeandwindow;


import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/*
上游给下游发送水位线使用广播，下游接受上游水位线以最小为准
* */

public class Flink02_WatermarkSend {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 5678);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        // 设置周期水位线间隔时间
        env.getConfig().setAutoWatermarkInterval(1000L);

        OutputTag<String> outputTag1 = new OutputTag<>("1", Types.STRING);
        OutputTag<String> outputTag2 = new OutputTag<>("2", Types.STRING);

        SingleOutputStreamOperator<String> ds = env.socketTextStream("localhost", 9999)
                .map(o -> o).name("m1")
                .returns(Types.STRING)
                .assignTimestampsAndWatermarks(new AWatermarkStrategy(0L))
                .map(o -> o).name("m2").setParallelism(2)
                .returns(Types.STRING);
        SingleOutputStreamOperator mainDs = ds.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String o, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                        if ("1".equals(o.split(" ")[0]))
                            context.output(outputTag1, o);
                        else if ("2".equals(o.split(" ")[0]))
                            context.output(outputTag2, o);
                        else
                            collector.collect(o);
                    }
                }
        );
        SingleOutputStreamOperator sideOutput1 = mainDs.getSideOutput(outputTag1)
                .assignTimestampsAndWatermarks(new AWatermarkStrategy(1000L));
        SingleOutputStreamOperator sideOutput2 = mainDs.getSideOutput(outputTag2)
                .assignTimestampsAndWatermarks(new AWatermarkStrategy(2000L));
        DataStream<String> unionDs = mainDs.union(sideOutput1, sideOutput2);
        unionDs.print();

        env.execute();
    }

    // 自定义WatermarkStrategy
    public static class AWatermarkStrategy<T> implements WatermarkStrategy<T> {
        // 记录已到数据最大值
        private Long maxTs = Long.MIN_VALUE;
        // 迟到时间
        private final Long delay;

        public AWatermarkStrategy(Long delay) {
            this.delay = delay;
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            // 自定义WatermarkGenerator
            return new WatermarkGenerator<T>() {

                // 给每条数据生成水位线
                @Override
                public void onEvent(T event, long eventTimestamp, WatermarkOutput watermarkOutput) {
                    maxTs = Math.max(maxTs, eventTimestamp - delay);
                    Watermark watermark = new Watermark(maxTs);
                    watermarkOutput.emitWatermark(watermark);
                    System.out.println("每条数据水位线: " + watermark);
                }

                // 周期性生成水位线，默认周期200ms
                @Override
                public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                    //Watermark watermark = new Watermark(maxTs);
                    //watermarkOutput.emitWatermark(watermark);
                    //System.out.println("周期水位线: " + watermark);
                }
            };
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            // 自定义TimestampAssigner
            return new TimestampAssigner<T>() {
                @Override
                public long extractTimestamp(T event, long recordTimestamp) {
                    return Long.parseLong(((String) event).split(" ")[1]);
                }
            };
        }
    }
}
