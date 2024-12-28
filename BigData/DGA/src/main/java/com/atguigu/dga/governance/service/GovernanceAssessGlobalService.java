package com.atguigu.dga.governance.service;

import com.atguigu.dga.governance.bean.GovernanceAssessGlobal;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 治理总考评表 服务类
 * </p>
 *
 * @author -
 * @since 2024-10-08
 */
@DS("dga")
public interface GovernanceAssessGlobalService extends IService<GovernanceAssessGlobal> {
    void calcGlobalScore(String assessDate);
}
