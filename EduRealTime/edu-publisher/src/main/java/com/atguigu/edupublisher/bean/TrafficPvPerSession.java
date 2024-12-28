package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficPvPerSession {
    // 来源
    String sc;
    // 各会话平均页面浏览数
    Double pvPerSession;
}
