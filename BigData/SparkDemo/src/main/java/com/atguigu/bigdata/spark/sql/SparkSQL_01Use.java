package com.atguigu.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.IOException;

public class SparkSQL_01Use {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        SparkConf conf = new SparkConf().setAppName("sparksql").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> json = sparkSession.read().json("SparkDemo/data/text.txt");
        Dataset<User> userDataset = json.map(
                (MapFunction<Row, User>) value -> new User(value.getAs("name"), value.getAs("age")),
                Encoders.bean(User.class)
        );
        userDataset.show();
        KeyValueGroupedDataset<String, User> groupedDataset = userDataset.groupByKey(
                (MapFunction<User, String>) value -> value.getName(),
                Encoders.STRING()
        );
        Dataset<Tuple2<String, User>> result = groupedDataset.reduceGroups(
                (ReduceFunction<User>) (v1, v2) -> new User(v1.getName(), Math.max(v1.getAge(), v2.getAge()))
        );
        result.show();
        sparkSession.close();
    }
}