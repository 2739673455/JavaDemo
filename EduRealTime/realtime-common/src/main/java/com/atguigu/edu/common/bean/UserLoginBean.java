package com.atguigu.edu.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    String curDate;
    // 新增用户数
    Long nuCt;
    // 回流用户数
    Long backCt;
    // 活跃用户数
    Long actCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
