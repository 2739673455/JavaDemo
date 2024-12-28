package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TestPaperStatsBean {
    String paperId;
    String paperTitle;
    Long uv;
    BigDecimal avgScore;
    Long avgDurationSec;
}
