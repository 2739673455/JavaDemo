package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficSvCt {
    // 来源
    String sc;
    // 会话数
    Integer svCt;
}
