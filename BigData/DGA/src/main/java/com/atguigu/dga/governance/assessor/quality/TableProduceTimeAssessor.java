package com.atguigu.dga.governance.assessor.quality;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.constant.DgaConstant;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.meta.bean.TDsTaskInstance;
import com.atguigu.dga.meta.service.TDsTaskInstanceService;
import com.baomidou.dynamic.datasource.annotation.DS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

@DS("dolphinscheduler")
@Component("TABLE_PRODUCE_TIME")
public class TableProduceTimeAssessor extends Assessor {
    @Autowired
    TDsTaskInstanceService tdsTaskInstanceService;
    @Autowired
    JdbcTemplate jdbcTemplate;

    private List<Map<String, Object>> executeSql(String sql) {
        return jdbcTemplate.queryForList(sql);
    }

    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws ParseException, IOException, URISyntaxException, InterruptedException {
        //排除ods表
        if (DgaConstant.DW_LEVEL_ODS.equals(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel())) {
            return;
        }
        //获取指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        Integer days = jsonObject.getInteger("days");
        Integer percent = jsonObject.getInteger("percent");
        //获取表名，考评日期
        String schemaName = assessParam.getTableMetaInfo().getSchemaName();
        String tableName = assessParam.getTableMetaInfo().getTableName();
        tableName = schemaName + "." + tableName;
        String assessDate = assessParam.getAssessDate();
        //获取当前任务时效
        TDsTaskInstance tdsTaskInstance = assessParam.getTdsTaskInstance();
        if (tdsTaskInstance == null) {
            return;
        }
        Long thisDuration = (tdsTaskInstance.getEndTime().getTime() - tdsTaskInstance.getStartTime().getTime()) / 1000;
        //获取前days天平均时效
        String sql = "select\n" +
                "name,\n" +
                "cast(avg(end_time - start_time) as signed) as avg_duration\n" +
                "from\n" +
                "t_ds_task_instance\n" +
                "where\n" +
                "id in (\n" +
                "select\n" +
                "max(id) as max_id\n" +
                "from\n" +
                "t_ds_task_instance\n" +
                "where\n" +
                "(cast('" + assessDate + "' as date) - interval " + days + " day) <= date(start_time)\n" +
                "and date_format(start_time, '%Y-%m-%d') < '" + assessDate + "'\n" +
                "and name = '" + tableName + "'\n" +
                "and state = 7\n" +
                "group by\n" +
                "date(start_time)\n" +
                ")\n" +
                "group by\n" +
                "name;";
        List<Map<String, Object>> duration = executeSql(sql);
        Long avgDuration = (Long) duration.get(0).get("avg_duration");

        if (avgDuration != null && 100 * (thisDuration - avgDuration) > percent * avgDuration) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("表产出时效过长");
            String comment1 = "当日时效:" + thisDuration + " 前" + days + "天平均时效:" + avgDuration;
            String comment2 = " 当日时效超出前" + days + "天平均时效的" + 100 * (thisDuration - avgDuration) / avgDuration + "%";
            governanceAssessDetail.setAssessComment(comment1 + comment2);
        }
    }
}
