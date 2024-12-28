package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TestCourseStatsBean {
    String courseId;
    String courseName;
    Long uv;
    BigDecimal avgScore;
    Long avgDurationSec;
}
