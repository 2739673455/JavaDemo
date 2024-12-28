package com.atguigu.flink.datastreamapi.transformation;

/*
普通函数类: 只定义了函数的具体功能
    MapFunction
    FilterFunction
    FlatMapFunction
    ReduceFunction
    ...

富函数类:
    RichFunction 富函数接口
        AbstractRichFunction 抽象富函数父类
            RichMapFunction
            RichFilterFunction
            RichFlatMapFunction
            RichReduceFunction
            ...
    富函数类的功能:
        1. 对数据处理的功能
        2. 生命周期方法，算子每一个并行实例的生命周期
            void open(Configuration parameters) throws Exception
                算子的实例对象在创建的时候会调用一次open方法(在数据处理之前，做一些前置工作)
            void close() throws Exception
                算子的实例对象在销毁的时候会调用一次close方法(在数据处理之后，做一些后置工作)
        3. 获取运行时上下文对象 RuntimeContext
            1. 获取一些和当前作业相关的信息
            2. 做状态编程
                getState()
                getListState()
                getReduceState()
                getAggregatingState()
                getMapState()
* */

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_UserDefineFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(3), WatermarkStrategy.noWatermarks(), "ds");
        ds
                .map(
                        new RichMapFunction<Event, Event>() {
                            @Override
                            public void open(Configuration parameters) {
                                System.out.println("get Jedis");
                            }

                            @Override
                            public Event map(Event event) {
                                return event;
                            }

                            @Override
                            public void close() {
                                System.out.println("close Jedis");
                            }
                        }
                )
                .print();

        env.execute();
    }
}
