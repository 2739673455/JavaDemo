package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CourseStarsStatsBean {
    BigDecimal avgStars;
    Long uv;
    BigDecimal star5Rate;
}
