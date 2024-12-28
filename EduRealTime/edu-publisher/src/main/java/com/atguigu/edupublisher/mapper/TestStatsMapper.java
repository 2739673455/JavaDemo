package com.atguigu.edupublisher.mapper;

import com.atguigu.edupublisher.bean.TestCourseStatsBean;
import com.atguigu.edupublisher.bean.TestPaperScoreDistributionStatsBean;
import com.atguigu.edupublisher.bean.TestPaperStatsBean;
import com.atguigu.edupublisher.bean.TestQuestionCorrectStatsBean;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TestStatsMapper {

    //1.5.1 各试卷考试统计
    @Select("select\n" +
            "paper_id as paperId,\n" +
            "paper_title as paperTitle,\n" +
            "sum(paper_user_count) as uv,\n" +
            "sum(score)/sum(view_count) as avgScore,\n" +
            "cast(sum(duration_sec)/sum(view_count) as bigint) as avgDurationSec\n" +
            "from edu.dws_test_course_paper_score_duration_window\n" +
            "partition par#{date}\n" +
            "group by paper_id,paper_title;")
    List<TestPaperStatsBean> selectTestPaperStats(@Param("date") Integer date);

    //1.5.2 各课程交易统计
    @Select("select\n" +
            "course_id as courseId,\n" +
            "course_name as courseName,\n" +
            "sum(course_user_count) as uv,\n" +
            "sum(score)/sum(view_count) as avgScore,\n" +
            "cast(sum(duration_sec)/sum(view_count) as bigint) as avgDurationSec\n" +
            "from edu.dws_test_course_paper_score_duration_window\n" +
            "partition par#{date}\n" +
            "group by course_id,course_name;")
    List<TestCourseStatsBean> selectTestCourseStats(@Param("date") Integer date);

    //1.5.3 各试卷成绩分布
    @Select("select    paper_id                                            as paperId,\n" +
            "          paper_title                                         as paperTitle,\n" +
            "          sum(paper_user_count)                               as uv,\n" +
            "          sum(score) / sum(view_count)                        as avgScore,\n" +
            "          cast(sum(duration_sec) / sum(view_count) as bigint) as avgDurationSec\n" +
            "from      edu.dws_test_course_paper_score_duration_window partition par#{date}\n" +
            "group by  paper_id,\n" +
            "          paper_title;")
    List<TestPaperScoreDistributionStatsBean> selectTestPaperScoreDistributionStats(@Param("date") Integer date);

    //1.5.4 答题情况统计
    @Select("select    question_id                           as questionId,\n" +
            "          question_txt                          as questionTxt,\n" +
            "          sum(correct_count)                    as correctCount,\n" +
            "          sum(`count`)                          as `count`,\n" +
            "          sum(correct_count) / sum(`count`)     as correctRate,\n" +
            "          sum(correct_uu_count)                 as correctUuCount,\n" +
            "          sum(uu_count)                         as uuCount,\n" +
            "          sum(correct_uu_count) / sum(uu_count) as correctUuRate\n" +
            "from      edu.dws_test_question_correct_uu_window partition par#{date}\n" +
            "group by  question_id,\n" +
            "          question_txt;")
    List<TestQuestionCorrectStatsBean> selectTestQuestionCorrectStats(@Param("date") Integer date);
}