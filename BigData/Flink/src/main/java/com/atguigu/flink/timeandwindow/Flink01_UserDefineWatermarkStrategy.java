package com.atguigu.flink.timeandwindow;

/*
用户自定义水位线生成策略

生成水位线的时机: Flink支持在任意位置生成水位线，但一般会选择在Source阶段生成水位线，下游的算子都可以感知时间

WatermarkStrategy:
    WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context)
        创建WatermarkGenerator对象，用于基于数据中提取好的时间生成水位线
        void onEvent(Event event, long eventTimestamp, WatermarkOutput watermarkOutput)
        void onPeriodicEmit(WatermarkOutput watermarkOutput)

    TimestampAssigner<T> createTimestampAssigner(Context context)
        创建TimestampAssigner对象，用于从数据中提取时间
        long extractTimestamp(T element, long recordTimestamp)

断点式生成水位线(给每条数据生成水位线) -- 数据量较少，数据不密集，单位时间内到达的数据少
周期性生成水位线 -- 数据量大，数据密集，单位时间内到达的数据多，数据与数据之间间隔小(ms级别)

有序流:
    数据时间有序
乱序流:
    数据时间无序，取最大时间
乱序流+迟到数据:
    生成水位线时减去迟到时间
* */

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Flink01_UserDefineWatermarkStrategy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 生成水位线周期
        env.getConfig().setAutoWatermarkInterval(1000L);
        SingleOutputStreamOperator<Event> ds = env.socketTextStream("localhost", 9999)
                .map(o -> new Event(o.split(" ")[0], o.split(" ")[1], Long.parseLong(o.split(" ")[2])));
        // 自定义水位线策略
        ds.assignTimestampsAndWatermarks(new AWatermarkStrategy(2000L));
        // Flink内置有序流水位线策略
        ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTs();
                            }
                        })
        );
        // Flink内置乱序流水位线策略
        ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(2000L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTs();
                            }
                        })
        );
        env.execute();
    }

    // 自定义有序流WatermarkStrategy
    public static class OrderedWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new OrderedWatermarkGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new OrderedTimestampAssigner();
        }
    }

    // 自定义有序流WatermarkGenerator
    public static class OrderedWatermarkGenerator implements WatermarkGenerator<Event> {
        // 记录已到数据最大值
        private Long maxTs = Long.MIN_VALUE;

        // 给每条数据生成水位线
        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput watermarkOutput) {
            Watermark watermark = new Watermark(eventTimestamp);
            watermarkOutput.emitWatermark(watermark);
            System.out.println("每条数据水位线: " + watermark);

            maxTs = Math.max(maxTs, eventTimestamp);
        }

        // 周期性生成水位线
        // 默认周期200ms
        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            Watermark watermark = new Watermark(maxTs);
            watermarkOutput.emitWatermark(watermark);
            System.out.println("周期水位线: " + watermark);
        }
    }

    // 自定义有序流TimestampAssigner
    public static class OrderedTimestampAssigner implements TimestampAssigner<Event> {
        @Override
        public long extractTimestamp(Event event, long recordTimestamp) {
            return event.getTs();
        }
    }

    // 自定义乱序流WatermarkStrategy
    public static class UnOrderedWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            // 自定义乱序流WatermarkGenerator
            return new WatermarkGenerator<Event>() {
                // 记录已到数据最大值
                private Long maxTs = Long.MIN_VALUE;
                private Long delay = 2000L;

                // 给每条数据生成水位线
                @Override
                public void onEvent(Event event, long eventTimestamp, WatermarkOutput watermarkOutput) {
                    maxTs = Math.max(maxTs, eventTimestamp) - delay;
                    //Watermark watermark = new Watermark(maxTs);
                    //watermarkOutput.emitWatermark(watermark);
                    //System.out.println("每条数据水位线: " + watermark);
                }

                // 周期性生成水位线
                // 默认周期200ms
                @Override
                public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                    Watermark watermark = new Watermark(maxTs);
                    watermarkOutput.emitWatermark(watermark);
                    System.out.println("周期水位线: " + watermark);
                }
            };
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            // 自定义乱序流TimestampAssigner
            return new TimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long recordTimestamp) {
                    return event.getTs();
                }
            };
        }
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
                    Watermark watermark = new Watermark(maxTs);
                    watermarkOutput.emitWatermark(watermark);
                    System.out.println("周期水位线: " + watermark);
                }
            };
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            // 自定义TimestampAssigner
            return new TimestampAssigner<T>() {
                @Override
                public long extractTimestamp(T event, long recordTimestamp) {
                    return ((Event) event).getTs();
                }
            };
        }
    }
}
