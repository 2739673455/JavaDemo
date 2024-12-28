package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {

        // 从Kafka的topic_db中读取数据创建动态表，过滤出评论数据 table='comment_info' and type='insert'
        readOdsDb(streamTableEnv, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        Table commentInfo = streamTableEnv.sqlQuery(
                "select " +
                        "`data`['id'] as id, " +
                        "`data`['user_id'] as user_id, " +
                        "`data`['sku_id'] as sku_id, " +
                        "`data`['appraise'] as appraise, " +
                        "`data`['comment_txt'] as comment_txt, " +
                        "ts, " +
                        "process_time, " +
                        "event_time " +
                        "from topic_db " +
                        "where `table`='comment_info' and `type`='insert' "
        );
        streamTableEnv.createTemporaryView("comment_info", commentInfo);

        // 从HBase中读取字典表数据创建动态表
        readDimDic(streamTableEnv);

        // 关联评论和字典表数据
        Table joinedTable = streamTableEnv.sqlQuery(
                "select " +
                        "id, " +
                        "user_id, " +
                        "sku_id, " +
                        "appraise, " +
                        "dic.dic_name as appraise_name, " +
                        "comment_txt, " +
                        "ts " +
                        "from comment_info as c " +
                        "join base_dic for system_time as of c.process_time as dic " +
                        "on c.appraise=dic.dic_code "
        );

        // 将关联结果输出到Kafka
        streamTableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " ( " +
                        "id string, " +
                        "user_id string, " +
                        "sku_id string, " +
                        "appraise string, " +
                        "appraise_name string, " +
                        "comment_txt string, " +
                        "ts bigint, " +
                        "primary key (id) not enforced " +
                        ") " +
                        SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)
        );
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
