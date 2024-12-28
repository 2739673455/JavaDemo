package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficUjRate {
    // 来源
    String ch;
    // 跳出率
    Double ujRate;
}
