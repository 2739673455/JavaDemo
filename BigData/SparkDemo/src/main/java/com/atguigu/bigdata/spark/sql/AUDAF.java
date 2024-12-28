package com.atguigu.bigdata.spark.sql;


import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import scala.Tuple2;

//继承org.apache.spark.sql.expressions.Aggregator
//定义泛型
//    IN:   函数的输入数据的类型
//    BUFF: 函数的缓冲区数据的类型
//        KV键值对只能访问不能修改
//    OUT:  函数的输出数据的类型


public class AUDAF extends Aggregator<Long, Tuple2<Long, Long>, Long> {

    @Override
    public Tuple2<Long, Long> zero() {
        return new Tuple2<>(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> reduce(Tuple2<Long, Long> b, Long a) {
        return new Tuple2<>(b._1 + a, b._2 + 1);
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> b1, Tuple2<Long, Long> b2) {
        return new Tuple2<>(b1._1 + b2._1, b1._2 + b2._2);
    }

    @Override
    public Long finish(Tuple2<Long, Long> reduction) {
        return reduction._1 / reduction._2;
    }

    @Override
    public Encoder<Tuple2<Long, Long>> bufferEncoder() {
        return Encoders.tuple(Encoders.LONG(), Encoders.LONG());
    }

    @Override
    public Encoder<Long> outputEncoder() {
        return Encoders.LONG();
    }
}