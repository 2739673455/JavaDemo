package com.atguigu.bigdata.spark.sql;

import org.apache.spark.sql.*;

import java.io.IOException;

public class SparkSQL_02UDAF {
    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("Java Spark SQL")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> jsonDS = sparkSession.read().json("SparkDemo/data/text.txt");
        jsonDS.createOrReplaceTempView("text");
//        使用LONG而非INT
//        自定义聚合对象，创建公共类
        sparkSession.udf().register("func", functions.udaf(new AUDAF(), Encoders.LONG()));
        sparkSession.sql("select func(age) from text").show();
        sparkSession.stop();
    }
}