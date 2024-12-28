package com.atguigu.edupublisher.mapper;

import com.atguigu.edupublisher.bean.CourseCategoryStatsBean;
import com.atguigu.edupublisher.bean.CourseOrderStatisticsBean;
import com.atguigu.edupublisher.bean.CourseStarsStatsBean;
import com.atguigu.edupublisher.bean.CourseSubjectStatsBean;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface CourseStatsMapper {
    // 1.3.2 评价情况
    @Select("select    cast(sum(review_stars) / sum(uv) as decimal(16, 2)) as avgStars,\n" +
            "          sum(uv)                                             as uv,\n" +
            "          cast(sum(star5) / sum(uv) as decimal(16, 2))        as star5Rate\n" +
            "from      edu.dws_interaction_review_window partition par#{date}\n" +
            "group by  course_id,\n" +
            "          course_name;")
    List<CourseStarsStatsBean> selectCourseStarsStats(Integer date);

	// 1.3.1 课程下单情况统计
    @Select("select\n" +
            "    course_id as courseId,\n" +
            "    course_name as courseName,\n" +
            "    COUNT(*) courseCount,\n" +
            "    sum(course_order) as courseOrder,\n" +
            "    sum(final_amount) as finalAmount\n" +
            "from edu.dws_order_situation partition par#{date}\n" +
            "group by course_id,course_name")
    List<CourseOrderStatisticsBean> selectCourseOrderStats(Integer date);

	// 1.3.1 类别下单情况统计
    @Select("select\n" +
            "    category_id as categoryId,\n" +
            "    category_name as categoryName,\n" +
            "    count(*) as categoryCount,\n" +
            "    sum(category_order) as categoryOrder,\n" +
            "    sum(final_amount) as categoryFinalAmount\n" +
            "from edu.dws_order_situation partition par#{date}\n" +
            "group by category_id,category_name\n")
    List<CourseCategoryStatsBean> selectCourseCategoryStats(Integer date);

	// 1.3.1 学科下单情况统计
    @Select("select\n" +
            "    subject_id as subjectId,\n" +
            "    subject_name as subjectName,\n" +
            "    count(*) as subjectCount,\n" +
            "    sum(subject_order) as subjectOrder,\n" +
            "    sum(final_amount) as subjectfinalAmount\n" +
            "from edu.dws_order_situation partition par#{date}\n" +
            "group by subject_id,subject_name")
    List<CourseSubjectStatsBean> selectCourseSubjectStats(Integer date);
}
