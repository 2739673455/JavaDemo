package com.atguigu.edu.realtime.dws.trade.source.order.function;

import com.atguigu.edu.common.util.SQLUtil;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SourceJoin {
    public static void readBaseSource(StreamTableEnvironment tableEnv) {


        tableEnv.executeSql("CREATE TABLE base_source(\n" +
                "   id string,\n" +
                "   info ROW<source_site string>,\n" +
                "   PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+ SQLUtil.getHBaseDDL("dim_base_source"));
        // tableEnv.executeSql("select id,source_site from base_source").print();
    }
}
