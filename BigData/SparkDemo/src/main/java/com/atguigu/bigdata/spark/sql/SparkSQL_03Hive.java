package com.atguigu.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkSQL_03Hive {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        SparkConf conf = new SparkConf().setAppName("sparksql").setMaster("local[*]");
        SparkSession spark = SparkSession.builder()
                .enableHiveSupport()
                .config(conf).getOrCreate();

        spark.sql("select area\n" +
                "     , product_name\n" +
                "     , max(click_num)                                                                        as click_num\n" +
                "     , concat_ws(', ', collect_list(result), concat(\"其他\", round(100 - sum(rate), 1), \"%\")) as result\n" +
                "from (select area, product_name, click_num, concat(city_name, rate, \"%\") result, rate\n" +
                "      from (select area, product_name, click_num, city_name, round(num / click_num * 100, 1) rate\n" +
                "            from (select *, rank() over (partition by area,product_name order by num desc) num_rank\n" +
                "                  from (select *\n" +
                "                             , (rank() over (partition by area order by click_num desc) - 1) /\n" +
                "                               count(*) over (partition by area,product_name) click_rank\n" +
                "                        from (select area\n" +
                "                                   , product_name\n" +
                "                                   , sum(num) over (partition by area, product_name) click_num\n" +
                "                                   , city_name\n" +
                "                                   , num\n" +
                "                              from (select click_product_id, city_id, count(*) num\n" +
                "                                    from user_visit_action\n" +
                "                                    where click_product_id <> -1\n" +
                "                                    group by click_product_id, city_id) t1\n" +
                "                                       join city_info on t1.city_id = city_info.city_id\n" +
                "                                       join product_info on t1.click_product_id = product_info.product_id) t2) t3\n" +
                "                  where click_rank <= 2) t4\n" +
                "            where num_rank <= 2) t5) t6\n" +
                "group by area, product_name\n" +
                "order by area, click_num desc;").show(100, false);
        spark.close();
    }
}