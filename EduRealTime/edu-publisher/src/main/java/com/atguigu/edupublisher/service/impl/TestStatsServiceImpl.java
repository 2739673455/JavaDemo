package com.atguigu.edupublisher.service.impl;

import com.atguigu.edupublisher.bean.TestCourseStatsBean;
import com.atguigu.edupublisher.bean.TestPaperScoreDistributionStatsBean;
import com.atguigu.edupublisher.bean.TestPaperStatsBean;
import com.atguigu.edupublisher.bean.TestQuestionCorrectStatsBean;
import com.atguigu.edupublisher.mapper.TestStatsMapper;
import com.atguigu.edupublisher.service.TestStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TestStatsServiceImpl implements TestStatsService {
    @Autowired
    TestStatsMapper testStatsMapper;

    @Override
    public List<TestPaperStatsBean> getTestPaperStats(Integer date) {
        return testStatsMapper.selectTestPaperStats(date);
    }

    @Override
    public List<TestCourseStatsBean> getTestCourseStats(Integer date) {
        return testStatsMapper.selectTestCourseStats(date);
    }

    @Override
    public List<TestPaperScoreDistributionStatsBean> getTestPaperScoreDistributionStats(Integer date) {
        return testStatsMapper.selectTestPaperScoreDistributionStats(date);
    }

    @Override
    public List<TestQuestionCorrectStatsBean> getTestQuestionCorrectStats(Integer date) {
        return testStatsMapper.selectTestQuestionCorrectStats(date);
    }
}
