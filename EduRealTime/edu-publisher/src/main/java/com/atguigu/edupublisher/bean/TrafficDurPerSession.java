package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficDurPerSession {
    // 来源
    String sc;
    // 各会话页面访问时长
    Double durPerSession;
}
