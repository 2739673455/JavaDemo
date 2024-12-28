package com.atguigu.flink.datastreamapi.source;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Flink06_UserDefineSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> ds = env.addSource(SourceUtil.getSourceFunction(3));
        ds.print();
        env.execute();
    }

    public static class EventSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> sourceContext) throws InterruptedException {
            String[] users = {"zhangsan", "lisi", "wangwu", "tom", "jerry", "alice"};
            String[] urls = {"/home", "/list", "/cart", "/order", "/pay"};

            while (true) {
                String user = users[RandomUtils.nextInt(0, users.length)];
                String url = urls[RandomUtils.nextInt(0, urls.length)];
                Long timestamp = System.currentTimeMillis();
                Event event = new Event(user, url, timestamp);

                sourceContext.collect(event);
                Thread.sleep(300);
            }
        }

        @Override
        public void cancel() {
        }
    }
}
