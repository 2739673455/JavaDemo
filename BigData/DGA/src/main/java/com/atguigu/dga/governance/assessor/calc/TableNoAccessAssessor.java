package com.atguigu.dga.governance.assessor.calc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Component("TABLE_NO_ACCESS")
public class TableNoAccessAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws ParseException {
        //获取指标参数
        JSONObject jsonObject = JSON.parseObject(assessParam.getGovernanceMetric().getMetricParamsJson());
        Integer days = jsonObject.getInteger("days");
        //获取最后访问日期，截断到天
        Date tableLastAccessTime = assessParam.getTableMetaInfo().getTableLastAccessTime();
        Date tableLastAccessDay = DateUtils.truncate(tableLastAccessTime, Calendar.DAY_OF_MONTH);
        //获取考评日期
        String assessDate = assessParam.getAssessDate();
        Date assessDay = DateUtils.parseDate(assessDate, "yyyy-MM-dd");
        //求相差天数
        long diffMs = Math.abs(assessDay.getTime() - tableLastAccessDay.getTime());
        long diffDays = TimeUnit.DAYS.convert(diffMs, TimeUnit.MILLISECONDS);

        if (days < diffDays) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("超过" + days + "天未访问");
            governanceAssessDetail.setAssessComment(diffDays + "天未访问");
        }
    }
}
