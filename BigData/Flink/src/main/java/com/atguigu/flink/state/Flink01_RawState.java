package com.atguigu.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/*
原始状态: 所有操作自己完成，Flink不进行任何管理
 * */
public class Flink01_RawState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置程序重启策略，指定重启次数与时间
        //env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(1)));
        // 开启检查点，程序会无限重启
        env.enableCheckpointing(2000L);

        env.socketTextStream("localhost", 9999)
                .map(
                        new MapFunction<String, String>() {
                            // 自定义状态(原始状态)
                            private List<String> dataList = new ArrayList<>();

                            @Override
                            public String map(String in) {
                                dataList.add(in);
                                return dataList.toString();
                            }
                        }
                )
                .addSink(
                        new SinkFunction<String>() {
                            @Override
                            public void invoke(String value, Context context) {
                                if (value.contains("100"))
                                    throw new RuntimeException("100异常");
                                System.out.println(value);
                            }
                        }
                );
        env.execute();
    }
}
