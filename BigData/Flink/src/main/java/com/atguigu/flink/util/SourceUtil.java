package com.atguigu.flink.util;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WaterSensor;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

//封装Source工具用于模拟数据
public class SourceUtil {

    public static DataGeneratorSource<Event> getDataGeneratorSource(double speed) {
        DataGeneratorSource<Event> dataGenSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, Event>() {
                    final String[] users = {"zhangsan", "lisi", "wangwu", "tom", "jerry", "alice"};
                    final String[] urls = {"/home", "/list", "/cart", "/order", "/pay"};

                    @Override
                    public Event map(Long aLong) {
                        String user = users[RandomUtils.nextInt(0, users.length)];
                        String url = urls[RandomUtils.nextInt(0, urls.length)];
                        Long timestamp = System.currentTimeMillis();
                        return new Event(user, url, timestamp);
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(speed),
                Types.POJO(Event.class)
        );
        return dataGenSource;
    }

    public static DataGeneratorSource<WaterSensor> getWaterSensorGeneratorSource(double speed) {
        return new DataGeneratorSource<>(
                (aLong) -> {
                    String id = RandomUtils.nextInt(0, 3) + "";
                    Long vc = RandomUtils.nextLong(80, 150);
                    Long ts = System.currentTimeMillis() - 1720000000000L;
                    return new WaterSensor(id, vc, ts);
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(speed),
                Types.POJO(WaterSensor.class)
        );
    }

    public static SourceFunction<Event> getSourceFunction(double speed) {
        long interval = (long) (1000D / speed);
        System.out.println(interval);
        return new SourceFunction<Event>() {
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
                    Thread.sleep(interval);
                }
            }

            @Override
            public void cancel() {
            }
        };
    }
}
