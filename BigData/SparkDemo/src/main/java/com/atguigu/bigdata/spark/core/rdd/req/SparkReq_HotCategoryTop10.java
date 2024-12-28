package com.atguigu.bigdata.spark.core.rdd.req;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;

public class SparkReq_HotCategoryTop10 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = jsc.textFile("SparkDemo/data/user_visit_action.txt", 5);
        rdd.filter(line -> "null".equals(line.split("_")[5]))
                .flatMapToPair(line -> {
                    String[] datas = line.split("_");
                    if (!"-1".equals(datas[6])) {
                        return Collections.singletonList(
                                new Tuple2<>(
                                        datas[6],
                                        new HotCategoryCount(datas[6], 1L, 0L, 0L)
                                )
                        ).iterator();
                    } else if (!"null".equals(datas[8])) {
                        String[] ids = datas[8].split(",");
                        ArrayList<Tuple2<String, HotCategoryCount>> arrs = new ArrayList<>();
                        for (String id : ids) {
                            arrs.add(
                                    new Tuple2<>(
                                            id,
                                            new HotCategoryCount(id, 0L, 1L, 0L)
                                    )
                            );
                        }
                        return arrs.iterator();
                    } else {
                        String[] ids = datas[10].split(",");
                        ArrayList<Tuple2<String, HotCategoryCount>> arrs = new ArrayList<>();
                        for (String id : ids) {
                            arrs.add(
                                    new Tuple2<>(
                                            id,
                                            new HotCategoryCount(id, 0L, 0L, 1L)
                                    )
                            );
                        }
                        return arrs.iterator();
                    }
                })
                .reduceByKey(HotCategoryCount::sumCount)
                .map(kv -> kv._2)
                .sortBy(item -> item, false, 20)
                .collect().forEach(System.out::println);

        jsc.stop();
    }
}

@AllArgsConstructor
@NoArgsConstructor
@Data
class HotCategoryCount implements Serializable, Comparable<HotCategoryCount> {
    private String cid;
    private Long click;
    private Long order;
    private Long pay;

    public static HotCategoryCount sumCount(HotCategoryCount h1, HotCategoryCount h2) {
        h1.setClick(h1.getClick() + h2.getClick());
        h1.setOrder(h1.getOrder() + h2.getOrder());
        h1.setPay(h1.getPay() + h2.getPay());
        return h1;
    }

    @Override
    public int compareTo(HotCategoryCount o) {
        if (this.click > o.click) {
            return 1;
        } else if (this.click < o.click) {
            return -1;
        } else {
            if (this.order > o.order) {
                return 1;
            } else if (this.order < o.order) {
                return -1;
            } else {
                return this.pay.compareTo(o.pay);
            }
        }
    }

    @Override
    public String toString() {
        return "cid=" + cid +
                "\t click=" + click +
                "\t order=" + order +
                "\t pay=" + pay;
    }
}