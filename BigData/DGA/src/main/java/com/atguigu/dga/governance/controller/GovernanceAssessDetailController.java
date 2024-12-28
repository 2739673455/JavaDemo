package com.atguigu.dga.governance.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga.governance.bean.GovernanceAssessTecOwner;
import com.atguigu.dga.governance.service.GovernanceAssessDetailService;
import com.atguigu.dga.governance.service.GovernanceAssessGlobalService;
import com.atguigu.dga.governance.service.GovernanceAssessTecOwnerService;
import com.atguigu.dga.governance.service.MainService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/governance")
public class GovernanceAssessDetailController {

    @Autowired
    GovernanceAssessGlobalService governanceAssessGlobalService;

    @Autowired
    GovernanceAssessTecOwnerService governanceAssessTecOwnerService;

    @Autowired
    GovernanceAssessDetailService governanceAssessDetailService;

    @Autowired
    MainService mainService;

    //全局分数接口
    @GetMapping(value = "/globalScore")
    public String globalScore() {
        GovernanceAssessGlobal governanceAssessGlobalScore = governanceAssessGlobalService.getOne(
                new QueryWrapper<GovernanceAssessGlobal>()
                        .inSql("assess_date", "select max(assess_date) from governance_assess_global")
        );
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("assessDate", governanceAssessGlobalScore.getAssessDate());
        jsonObject.put("sumScore", governanceAssessGlobalScore.getScore());
        List<BigDecimal> scoreList = Arrays.asList(
                governanceAssessGlobalScore.getScoreSpec(),
                governanceAssessGlobalScore.getScoreStorage(),
                governanceAssessGlobalScore.getScoreCalc(),
                governanceAssessGlobalScore.getScoreQuality(),
                governanceAssessGlobalScore.getScoreSecurity()
        );
        jsonObject.put("scoreList", scoreList);
        return jsonObject.toJSONString();
    }

    //治理考评榜
    @GetMapping(value = "/rankList")
    public String rankList() {
        List<Map<String, Object>> listMaps = governanceAssessTecOwnerService.listMaps(
                new QueryWrapper<GovernanceAssessTecOwner>()
                        .select("tec_owner as tecOwner", "score")
                        .inSql("assess_date", "select max(assess_date) from governance_assess_tec_owner")
                        .orderByDesc("score")
        );
        return JSON.toJSONString(listMaps);
    }

    //各个治理类型问题个数
    @GetMapping(value = "/problemNum")
    public String problemNum() {
        List<Map<String, Object>> listMaps = governanceAssessDetailService.listMaps(
                new QueryWrapper<GovernanceAssessDetail>()
                        .select("governance_type", "sum(if(assess_score < 10, 1, 0)) as problem_num")
                        .inSql("assess_date", "select max(assess_date) from governance_assess_detail")
                        .groupBy("governance_type")
        );

        //将governance_type作为key，将problem_num作为value
        Map<Object, Object> resultMap = listMaps.stream()
                .collect(Collectors.toMap(
                        map -> map.get("governance_type"),
                        map -> map.get("problem_num"))
                );
        return JSON.toJSONString(resultMap);
    }

    //治理问题项
    @GetMapping(value = "/problemList/{governType}/{pageNo}/{pageSize}")
    public String problemList(@PathVariable("governType") String governType,
                              @PathVariable("pageNo") Integer pageNo,
                              @PathVariable("pageSize") Integer pageSize) {

        //分页开始位置
        Integer start = (pageNo - 1) * pageSize;

        List<GovernanceAssessDetail> problemList = governanceAssessDetailService.list(
                new QueryWrapper<GovernanceAssessDetail>()
                        .eq("governance_type", governType)
                        .lt("assess_score", 10)
                        .inSql("assess_date", "select max(assess_date) from governance_assess_detail")
                        .last("limit " + start + "," + pageSize)
        );
        return JSON.toJSONString(problemList);
    }

    //重新评估
    @PostMapping(value = "/assess/{date}")
    public void reAssess(@PathVariable("date") String date) {
        mainService.startGovernanceAssess(date);
    }
}
