package com.atguigu.edupublisher.service.impl;

import com.atguigu.edupublisher.bean.CourseCategoryStatsBean;
import com.atguigu.edupublisher.bean.CourseOrderStatisticsBean;
import com.atguigu.edupublisher.bean.CourseStarsStatsBean;
import com.atguigu.edupublisher.bean.CourseSubjectStatsBean;
import com.atguigu.edupublisher.mapper.CourseStatsMapper;
import com.atguigu.edupublisher.service.CourseStarsStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CourseStarsStatsServiceImpl implements CourseStarsStatsService {

    @Autowired
    CourseStatsMapper courseStatsMapper;

    @Override
    public List<CourseStarsStatsBean> getCourseStarsStats(Integer date) {
        return courseStatsMapper.selectCourseStarsStats(date);
    }

	@Override
	public List<CourseOrderStatisticsBean> getCourseOrderStats(Integer date) {
		return courseStatsMapper.selectCourseOrderStats(date);
	}

	@Override
	public List<CourseCategoryStatsBean> getCourseCategoryStats(Integer date) {
		return courseStatsMapper.selectCourseCategoryStats(date);
	}

	@Override
	public List<CourseSubjectStatsBean> getCourseSubjectStats(Integer date) {
		return courseStatsMapper.selectCourseSubjectStats(date);
	}
}
