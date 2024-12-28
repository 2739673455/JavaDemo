package com.atguigu.dga.governance.assessor;

import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.Date;

public abstract class Assessor {

    public final GovernanceAssessDetail doAssess(AssessParam assessParam) {
        //添加考评基本信息
        GovernanceAssessDetail governanceAssessDetail = GovernanceAssessDetail.builder()
                .assessDate(assessParam.getAssessDate())
                .tableName(assessParam.getTableMetaInfo().getTableName())
                .schemaName(assessParam.getTableMetaInfo().getSchemaName())
                .metricId(assessParam.getGovernanceMetric().getId().toString())
                .metricName(assessParam.getGovernanceMetric().getMetricName())
                .governanceType(assessParam.getGovernanceMetric().getGovernanceType())
                .tecOwner(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getTecOwnerUserName())
                .assessScore(BigDecimal.TEN) //默认满分
                .createTime(new Date())
                //.assessProblem() 查找问题过程中根据实际情况给问题描述
                //.assessComment() 查找问题过程中根据实际情况给考评备注
                .build();
        try {
            checkProblem(assessParam, governanceAssessDetail);
        } catch (Exception e) {
            //若有异常，添加异常信息
            governanceAssessDetail.setIsAssessException("1");
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            e.printStackTrace(printWriter);
            e.printStackTrace();
            governanceAssessDetail.setAssessExceptionMsg(stringWriter.toString().substring(0, Math.min(stringWriter.toString().length(), 2000)));
        }
        //若有问题且有治理链接，添加治理链接
        if (governanceAssessDetail.getAssessScore().compareTo(BigDecimal.TEN) < 0 && assessParam.getGovernanceMetric().getGovernanceUrl() != null) {
            String governanceUrl = assessParam.getGovernanceMetric().getGovernanceUrl();
            governanceUrl = governanceUrl.replace("{tableId}", assessParam.getTableMetaInfo().getId().toString());
            governanceAssessDetail.setGovernanceUrl(governanceUrl);
        }
        return governanceAssessDetail;
    }

    public abstract void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws Exception;
}
