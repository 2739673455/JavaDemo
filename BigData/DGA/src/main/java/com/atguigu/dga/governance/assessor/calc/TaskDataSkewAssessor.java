package com.atguigu.dga.governance.assessor.calc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.constant.DgaConstant;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.meta.bean.TDsTaskInstance;
import com.atguigu.dga.util.HttpUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Component("TABLE_DATA_SKEW")
public class TaskDataSkewAssessor extends Assessor {

    @Value("${spark.history.server.url}")
    private String sparkHistoryServerUrl;

    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws Exception {
        //排除ods表
        if (DgaConstant.DW_LEVEL_ODS.equals(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel()))
            return;
        //获取指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        Long percent = jsonObject.getLong("percent");
        Long stageDurSeconds = jsonObject.getLong("stage_dur_seconds");
        //获取yarnId
        TDsTaskInstance tdsTaskInstance = assessParam.getTdsTaskInstance();
        if (tdsTaskInstance == null) return;
        String yarnId = tdsTaskInstance.getAppLink();
        //从spark历史服务器中提取yarnId对应的任务信息
        //获取yarnId对应的任务信息中 completed=true 的attemptId
        String urlYarnId = sparkHistoryServerUrl + "/" + yarnId;
        String response = HttpUtil.get(urlYarnId);
        jsonObject = JSON.parseObject(response);
        JSONArray attempts = jsonObject.getJSONArray("attempts");
        String attemptId = null;
        for (Object attempt : attempts) {
            if (((JSONObject) attempt).getBoolean("completed"))
                attemptId = ((JSONObject) attempt).getString("attemptId");
        }
        if (attemptId == null) return;
        //获取 status=COMPLETE 的阶段
        String urlAttemptId = urlYarnId + "/" + attemptId + "/stages";
        response = HttpUtil.get(urlAttemptId);
        List<JSONObject> stages = JSON.parseArray(response, JSONObject.class);
        List<String> stageIdList = new ArrayList<>();
        stages.forEach(stage -> {
            if ("COMPLETE".equals(stage.getString("status"))) stageIdList.add(stage.getString("stageId"));
        });
        if (stageIdList.isEmpty()) return;
        //创建stageUrl集合来存储存在数据倾斜的stage
        List<MyStage> skewStageList = new ArrayList<>();
        //获取每阶段中的task信息，封装到MyStage对象中
        String urlStageId;
        for (String stageId : stageIdList) {
            urlStageId = urlYarnId + "/" + attemptId + "/stages" + "/" + stageId;
            response = HttpUtil.get(urlStageId);
            //获取stage列表
            JSONObject stage = JSON.parseArray(response, JSONObject.class).get(0);
            JSONObject tasks = stage.getJSONObject("tasks");
            Long maxDuration = 0L;
            Long totalDuration = 0L;
            Long count = 0L;
            for (String taskKey : tasks.keySet()) {
                JSONObject valueJson = tasks.getJSONObject(taskKey);
                if ("SUCCESS".equals(valueJson.getString("status"))) {
                    Long duration = valueJson.getLong("duration");
                    maxDuration = Math.max(maxDuration, duration);
                    totalDuration += duration;
                    count++;
                }
            }
            if (maxDuration <= stageDurSeconds || count <= 1) continue;
            Long avgDuration = (totalDuration - maxDuration) / count;
            Long thisPercent = (maxDuration - avgDuration) * 100 / avgDuration;
            if (thisPercent > percent) {
                skewStageList.add(new MyStage(urlStageId, maxDuration, avgDuration, thisPercent));
            }
        }

        if (!skewStageList.isEmpty()) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在数据倾斜");
            StringBuilder comment = new StringBuilder();
            for (MyStage myStage : skewStageList) {
                comment.append("阶段url:").append(myStage.getUrl());
                comment.append(",最大时间").append(myStage.getMaxDuration());
                comment.append(",平均时间").append(myStage.getAvgDuration());
                comment.append(",超过比率").append(myStage.getPercent());
                comment.append("\n");
            }
            governanceAssessDetail.setAssessComment(comment.toString());
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public class MyStage {
        private String url;
        private Long maxDuration;
        private Long avgDuration;
        private Long percent;
    }
}
