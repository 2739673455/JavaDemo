package com.atguigu.dga.governance.service;

import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 治理考评结果明细 服务类
 * </p>
 *
 * @author -
 * @since 2024-09-27
 */
public interface GovernanceAssessDetailService extends IService<GovernanceAssessDetail> {
    //核心的考评方法
    void mainAccess(String assessDate);
}
