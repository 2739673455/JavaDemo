package com.atguigu.dga.governance.assessor.storage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.constant.DgaConstant;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;

@Component("TABLE_LIFECYCLE")
public class TableLifecycleAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws ParseException {
        //获取表生命周期
        String lifecycleType = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleType();
        Long lifecycleDays = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleDays();
        //判断是否设定生命周期类型
        if (lifecycleType == null || DgaConstant.LIFECYCLE_TYPE_UNSET.equals(lifecycleType)) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("未设置生命周期类型");
            return;
        }
        //获取指标生命周期
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        Long days = jsonObject.getLong("days");
        //判断是否为日分区
        if (DgaConstant.LIFECYCLE_TYPE_DAY.equals(lifecycleType)) {
            String partitionColNameJson = assessParam.getTableMetaInfo().getPartitionColNameJson();
            //判断是否存在分区字段
            if (partitionColNameJson == null || JSON.parseArray(partitionColNameJson, JSONObject.class).isEmpty()) {
                governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                governanceAssessDetail.setAssessProblem("日分区表未设置分区字段");
            }//判断是否存在生命周期天数
            else if (lifecycleDays == null || lifecycleDays == -1L) {
                governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
                governanceAssessDetail.setAssessProblem("日分区表未设定生命周期天数");
            }//判断生命周期是否小于指标
            else if (days < lifecycleDays) {
                governanceAssessDetail.setAssessScore(BigDecimal.valueOf(days * 10 / lifecycleDays));
                governanceAssessDetail.setAssessProblem("设定的生命周期天数大于建议的生命周期天数");
                governanceAssessDetail.setAssessComment("建议天数：" + days + "，实际天数：" + lifecycleDays);
            }
        }
    }
}