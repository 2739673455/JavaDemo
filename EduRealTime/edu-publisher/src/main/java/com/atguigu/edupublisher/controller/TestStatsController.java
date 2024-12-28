package com.atguigu.edupublisher.controller;

import com.atguigu.edupublisher.bean.*;
import com.atguigu.edupublisher.service.TestStatsService;
import com.atguigu.edupublisher.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class TestStatsController {
    @Autowired
    TestStatsService testStatsService;

    @RequestMapping("/test_paper_stats")
    public String testPaperStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0)
            date = DateFormatUtil.now();
        List<TestPaperStatsBean> beans = testStatsService.getTestPaperStats(date);
        StringBuilder result = new StringBuilder("{\"status\": 0,\"data\": {");
        StringBuilder paperTitleStr = new StringBuilder().append("\"categories\": [");
        StringBuilder courseUserCountStr = new StringBuilder("{\"name\": \"考试人数\",\"data\": [");
        StringBuilder avgScoreStr = new StringBuilder("{\"name\": \"平均分\",\"data\": [");
        StringBuilder avgDurationSecStr = new StringBuilder("{\"name\": \"平均时长\",\"data\": [");
        for (TestPaperStatsBean bean : beans) {
            paperTitleStr.append("\"").append(bean.getPaperTitle()).append("\",");
            courseUserCountStr.append(bean.getUv()).append(",");
            avgScoreStr.append(bean.getAvgScore()).append(",");
            avgDurationSecStr.append(bean.getAvgDurationSec()).append(",");
        }
        paperTitleStr.deleteCharAt(paperTitleStr.length() - 1);
        courseUserCountStr.deleteCharAt(courseUserCountStr.length() - 1);
        avgScoreStr.deleteCharAt(avgScoreStr.length() - 1);
        avgDurationSecStr.deleteCharAt(avgDurationSecStr.length() - 1);
        result.append(paperTitleStr).append("],")
                .append("\"series\": [")
                .append(courseUserCountStr).append("]},")
                .append(avgScoreStr).append("]},")
                .append(avgDurationSecStr).append("]}")
                .append("]}}");
        return result.toString();
    }

    @RequestMapping("/test_course_stats")
    public String testCourseStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0)
            date = DateFormatUtil.now();
        List<TestCourseStatsBean> result = testStatsService.getTestCourseStats(date);

		return result.toString();
    }

    @RequestMapping("/test_paper_score_distribution_stats")
    public String testPaperScoreDistributionStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0)
            date = DateFormatUtil.now();
        List<TestPaperScoreDistributionStatsBean> result = testStatsService.getTestPaperScoreDistributionStats(date);
        return result.toString();
    }

    @RequestMapping("/test_question_correct_stats")
    public String testQuestionCorrectStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0)
            date = DateFormatUtil.now();
        List<TestQuestionCorrectStatsBean> result = testStatsService.getTestQuestionCorrectStats(date);
        return result.toString();
    }
}
