package com.atguigu.flink.sql.wordcount;


import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
Flink TableAPI/SQL 基本使用
编码步骤:
    1. 准备流表执行环境
    2. 读取数据
    3. 建表
    4. 使用TableAPI/SQL对表的数据进行处理
    5. 输出结果
* */
public class Flink01_TableAPIAndSQLTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // 通过流环境读取数据
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("localhost", 9999)
                .map(o -> new WaterSensor(o.split(" ")[0], Long.parseLong(o.split(" ")[1]), Long.parseLong(o.split(" ")[2])));
        // 流转表
        //  流转表时，Flink会根据流中数据结构来确定表结构，如果流中数据是POJO类型，转成表之后POJO属性会作为表的字段，POJO属性类型会作为表字段类型
        Table table = streamTableEnv.fromDataStream(ds);
        // 打印表结构
        table.printSchema();


        // TableAPI: 对表数据处理，从表中查询水位>=100的数据
        //Table resultTable = table.where(Expressions.$("vc").isGreaterOrEqual(100))
        //        .select(Expressions.$("id"), Expressions.$("vc"), Expressions.$("ts"));

        // SQL: 将table对象注册到表环境中
        streamTableEnv.createTemporaryView("t1", table);
        // SQL: 对表数据处理，从表中查询水位>=100的数据
        Table resultTable = streamTableEnv.sqlQuery("select id,vc,ts from t1 where vc>=100");


        resultTable.execute().print();
    }
}
