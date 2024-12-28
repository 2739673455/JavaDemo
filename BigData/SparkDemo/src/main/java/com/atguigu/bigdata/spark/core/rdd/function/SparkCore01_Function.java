package com.atguigu.bigdata.spark.core.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkCore01_Function {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark RDD Function");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4));
//        RDD模型提供的方法会根据数据类型进行区分
//        单值类型
//        键值类型
//            KV，new Tuple2<Key,Value>()

//        map : 将指定的数据转换为另一个数据
        rdd.map(num -> new Tuple2<>(num, (Integer) num % 2))
//        filter : 过滤
                .filter(num -> (Integer) ((Tuple2) num)._2 == 0)
//        flatMap : 扁平化，返回可迭代对象
                .flatMap(tuple -> Arrays.asList(tuple._1, tuple._2).iterator())
//        groupBy : 分组，对数据添加分组标记，标记相同的分到一组
//            在底层实现时，会将一个分区的数据打乱后和其他分区的数据重新组合，该操作称之为shuffle
//            Spark要求一个组的数据必须在同一分区中
//            分区数量可能不合理
//            Spark会将已经分组的数据进行落盘处理
//            shuffle操作会影响Spark的计算性能
//            shuffle操作一般都会提供改变分区的功能
                .groupBy(num -> num)
//        distinct : 去重
//            distinct底层存在shuffle操作
                .distinct()
//        sortBy : 排序
//            第一个参数:排序的规则
//                给每一个数据增加排序标记，根据标记大小对数据排序
//            第二个参数:升序或降序
//            第三个参数:分区数量的设定
                .sortByKey()
//        coalesce : 合并
//            默认没有shuffle操作，数据不会打乱重组
//            可能存在数据倾斜，若倾斜可以使用shuffle操作
                .coalesce(1)
//        repartition : 重新分区
                .repartition(3)

                .collect().forEach(System.out::println);
        jsc.stop();
    }
}