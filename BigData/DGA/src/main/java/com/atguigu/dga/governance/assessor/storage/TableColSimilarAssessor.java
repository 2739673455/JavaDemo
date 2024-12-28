package com.atguigu.dga.governance.assessor.storage;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.meta.bean.TableMetaInfo;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Component("TABLE_COL_SIMILAR")
public class TableColSimilarAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws ParseException {
        //获取指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject metricParamsJsonObject = JSON.parseObject(metricParamsJson);
        Long percent = metricParamsJsonObject.getLong("percent");
        //获取此表所有字段
        List<String> thisColList = getColList(assessParam.getTableMetaInfo());
        //获取同层级除自己以外所有表
        String dwLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel();
        List<TableMetaInfo> otherTables = assessParam.getTableMetaInfoList().stream()
                .filter(tableMetaInfo -> tableMetaInfo.getTableMetaInfoExtra().getDwLevel().equals(dwLevel)
                        && !tableMetaInfo.getTableName().equals(assessParam.getTableMetaInfo().getTableName())
                ).collect(Collectors.toList());

        List<String> sameTableList = new ArrayList<>();
        for (TableMetaInfo tableMetaInfo : otherTables) {
            List<String> otherColList = getColList(tableMetaInfo);
            Collection sameColList = CollectionUtils.intersection(thisColList, otherColList);
            if (sameColList.size() * 100L / thisColList.size() > percent) {
                sameTableList.add(tableMetaInfo.getTableName());
            }
        }
        if (!sameTableList.isEmpty()) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在相似表");
            governanceAssessDetail.setAssessComment("相似表为：" + String.join("\n", sameTableList));
        }
    }

    //从tableMetaInfo中提取字段
    private static List<String> getColList(TableMetaInfo tableMetaInfo) {
        List<JSONObject> colObjectList = JSON.parseArray(tableMetaInfo.getColNameJson(), JSONObject.class);
        return colObjectList.stream().map(o -> o.getString("name")).collect(Collectors.toList());
    }
}
