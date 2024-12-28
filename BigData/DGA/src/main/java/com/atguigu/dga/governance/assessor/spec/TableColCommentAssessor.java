package com.atguigu.dga.governance.assessor.spec;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Component("TABLE_COL_COMMENT")
public class TableColCommentAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) {
        //方式一: 转换为JSON对象，逐个判断是否存在comment
        //  获取字段信息列表
        List<JSONObject> jsonObjectList = JSON.parseArray(assessParam.getTableMetaInfo().getColNameJson(), JSONObject.class);
        //  获取缺少字段备注的字段名列表
        List<String> problemColNameList = jsonObjectList.stream()
                .filter(jsonObject -> jsonObject.getString("comment") == null || jsonObject.getString("comment").trim().isEmpty())
                .map(jsonObject -> jsonObject.getString("name"))
                .collect(Collectors.toList());
        if (!problemColNameList.isEmpty()) {
            governanceAssessDetail.setAssessScore(BigDecimal.valueOf(10L - 10L * problemColNameList.size() / jsonObjectList.size()));
            governanceAssessDetail.setAssessProblem("缺少字段备注");
            governanceAssessDetail.setAssessComment("缺少备注的字段：" + problemColNameList);
        }

        //方式二: 正则表达式处理
        //String colNameJson = assessParam.getTableMetaInfo().getColNameJson();
        //int totalNum = 0; //总字段数
        //int problemNum = 0; //没有备注的字段数
        //ArrayList<String> problemColNameList = new ArrayList<>();
        //Pattern compile = Pattern.compile("\\{(.*?)\"name\":\"(.*?)\",\"type\":\".*?\"}");
        //Matcher matcher = compile.matcher(colNameJson);
        //while (matcher.find()) {
        //    totalNum++;
        //    if (matcher.group(1).isEmpty()) {
        //        problemNum++;
        //        problemColNameList.add(matcher.group(2));
        //    }
        //}
        //if (problemNum != 0) {
        //    governanceAssessDetail.setAssessScore(BigDecimal.valueOf(10L - 10L * problemNum / totalNum));
        //    governanceAssessDetail.setAssessProblem("缺少字段备注");
        //    governanceAssessDetail.setAssessComment("缺少备注的字段为：" + problemColNameList);
        //}
    }
}
