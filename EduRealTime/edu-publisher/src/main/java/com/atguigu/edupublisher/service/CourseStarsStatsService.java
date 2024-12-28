package com.atguigu.edupublisher.service;

import com.atguigu.edupublisher.bean.CourseCategoryStatsBean;
import com.atguigu.edupublisher.bean.CourseOrderStatisticsBean;
import com.atguigu.edupublisher.bean.CourseStarsStatsBean;
import com.atguigu.edupublisher.bean.CourseSubjectStatsBean;

import java.util.List;

public interface CourseStarsStatsService {
    List<CourseStarsStatsBean> getCourseStarsStats(Integer date);

	List<CourseOrderStatisticsBean> getCourseOrderStats(Integer date);

	List<CourseCategoryStatsBean> getCourseCategoryStats(Integer date);

	List<CourseSubjectStatsBean> getCourseSubjectStats(Integer date);
}
