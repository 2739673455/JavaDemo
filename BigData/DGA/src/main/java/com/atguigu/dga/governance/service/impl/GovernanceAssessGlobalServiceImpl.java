package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.governance.bean.GovernanceAssessGlobal;
import com.atguigu.dga.governance.mapper.GovernanceAssessGlobalMapper;
import com.atguigu.dga.governance.service.GovernanceAssessGlobalService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 * 治理总考评表 服务实现类
 * </p>
 *
 * @author -
 * @since 2024-10-08
 */
@DS("dga")
@Service
public class GovernanceAssessGlobalServiceImpl extends ServiceImpl<GovernanceAssessGlobalMapper, GovernanceAssessGlobal> implements GovernanceAssessGlobalService {
    @Override
    public void calcGlobalScore(String assessDate) {
        GovernanceAssessGlobal governanceAssessGlobalScore = getBaseMapper().selectGovernanceAssessGlobal(assessDate);
        this.remove(new QueryWrapper<GovernanceAssessGlobal>().eq("assess_date", assessDate));
        this.save(governanceAssessGlobalScore);
    }
}
