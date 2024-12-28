package com.atguigu.edupublisher.service;

import com.atguigu.edupublisher.bean.TestCourseStatsBean;
import com.atguigu.edupublisher.bean.TestPaperScoreDistributionStatsBean;
import com.atguigu.edupublisher.bean.TestPaperStatsBean;
import com.atguigu.edupublisher.bean.TestQuestionCorrectStatsBean;

import java.util.List;

public interface TestStatsService {
    List<TestPaperStatsBean> getTestPaperStats(Integer date);

    List<TestCourseStatsBean> getTestCourseStats(Integer date);

    List<TestPaperScoreDistributionStatsBean> getTestPaperScoreDistributionStats(Integer date);

    List<TestQuestionCorrectStatsBean> getTestQuestionCorrectStats(Integer date);
}
