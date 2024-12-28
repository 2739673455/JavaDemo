package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserFunnelStatsBean {
    Long homeCount;
    Long courseDetailCount;
    Long cartAddCount;
    Long orderCount;
    Long paySuccessCount;
}
