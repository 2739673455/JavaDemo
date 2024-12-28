package com.atguigu.dga.governance.service;

import com.atguigu.dga.governance.bean.GovernanceAssessTable;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 表治理考评情况 服务类
 * </p>
 *
 * @author -
 * @since 2024-10-08
 */
@DS("dga")
public interface GovernanceAssessTableService extends IService<GovernanceAssessTable> {
    void calcTableScore(String assessDate);
}
