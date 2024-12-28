package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TestQuestionCorrectStatsBean {
    Long questionId;
    String questionTxt;
    Long correctCount;
    Long count;
    BigDecimal correctRate;
    Long correctUuCount;
    Long uuCount;
    BigDecimal correctUuRate;
}
