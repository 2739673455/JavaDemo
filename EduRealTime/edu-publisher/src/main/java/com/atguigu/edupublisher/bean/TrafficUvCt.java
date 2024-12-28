package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TrafficUvCt {
    //来源
    String sc;
    //独立访客数
    Integer uvCt;
}
