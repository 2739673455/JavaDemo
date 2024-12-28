package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TestPaperScoreDistributionStatsBean {
    Long paperId;
    String paperTitle;
    String scoreRange;
    Long uv;
}
