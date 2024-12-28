package com.atguigu.flink.datastreamapi.transformation;

/*
处理函数功能:
    1. 对数据处理的方法 processElement()，在不同处理函数中，该方法略有不同
    2. 继承AbstractRichFunction类，拥有富函数功能，如生命周期方法和状态编程
    3. 定时器编程
        onTimer(): 定时器触发后，会调用该方法
        TimerService(): 用于注册、删除定时器
            currentProcessingTime()  获取当前的处理时间
            registerProcessingTimeTimer(long time)  注册处理时间定时器，当处理时间超过time时触发
            deleteProcessingTimeTimer(long time)  删除触发时间为time的处理时间定时器
            currentWatermark()  获取当前的水位线(事件时间)
            registerEventTimeTimer(long time)  注册事件时间定时器，当水位线超过time时触发
            deleteEventTimeTimer(long time)  删除触发时间为time的事件时间定时器
            注意:
                只有基于KeyedStream的处理函数，才能注册和删除定时器；未作按键分区的DataStream不支持定时器操作，只能获取当前时间
                不同key在同一时间注册定时器，未来每个key的定时器都会触发
                同一个key在同一时间注册多个定时器，未来只会触发一个
    4. 侧输出流
        output()

常见处理函数:
    1. ProcessFunction
        DataStream.process()
    2. KeyedProcessFunction
        KeyedStream.process()
    3. ProcessWindowFunction
        WindowedStream.process()
    4. ProcessAllWindowFunction
        AllWindowedStream.process()
    5. CoProcessFunction
        ConnectedStream.process()
    6. ProcessJoinFunction
        IntervalJoined.process()
    7. BroadcastProcessFunction
        BroadcastConnectedStream.process()
    8. KeyedBroadcastProcessFunction
        BroadcastConnectedStream.process()
* */

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink07_ProcessOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
                .map(o -> new Event(o.split(" ")[0], o.split(" ")[1], Long.parseLong(o.split(" ")[2])))
                .returns(Types.POJO(Event.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, l) -> event.getTs())
                )
                .keyBy(Event::getUser)
                .process(
                        new KeyedProcessFunction<String, Event, Event>() {
                            @Override
                            public void processElement(Event event, KeyedProcessFunction<String, Event, Event>.Context context, Collector<Event> collector) {
                                long time = context.timerService().currentProcessingTime();
                                System.out.println("处理时间: " + time);

                                long watermark = context.timerService().currentWatermark();
                                System.out.println("水位线: " + watermark);

                                context.timerService().registerProcessingTimeTimer(time + 2000);
                                System.out.println("注册的处理时间定时器: " + (time + 2000));

                                context.timerService().registerEventTimeTimer(event.getTs() - 1 + 2000);
                                System.out.println("注册的事件时间定时器: " + (event.getTs() - 1 + 2000));

                                collector.collect(event);
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, Event>.OnTimerContext ctx, Collector<Event> out) {
                                System.out.println("定时器触发: " + timestamp);
                            }
                        }
                )
                .print();
        env.execute();
    }
}
