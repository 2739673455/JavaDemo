package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.constant.DgaConstant;
import com.atguigu.dga.governance.bean.GovernanceAssessTable;
import com.atguigu.dga.governance.bean.GovernanceType;
import com.atguigu.dga.governance.mapper.GovernanceAssessTableMapper;
import com.atguigu.dga.governance.service.GovernanceAssessTableService;
import com.atguigu.dga.governance.service.GovernanceTypeService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>
 * 表治理考评情况 服务实现类
 * </p>
 *
 * @author -
 * @since 2024-10-08
 */
@DS("dga")
@Service
public class GovernanceAssessTableServiceImpl extends ServiceImpl<GovernanceAssessTableMapper, GovernanceAssessTable> implements GovernanceAssessTableService {

    @Autowired
    GovernanceTypeService governanceTypeService;

    @Override
    public void calcTableScore(String assessDate) {
        //获取每张表各项指标平均分
        List<GovernanceAssessTable> governanceAssessTableList = getBaseMapper().selectGovernanceAssessTableList(assessDate);
        //获取指标类型
        List<GovernanceType> governanceTypeList = governanceTypeService.list();
        //将指标类型放入Map
        Map<String, BigDecimal> governanceTypeWeightMap = governanceTypeList.stream().collect(Collectors.toMap(GovernanceType::getTypeCode, GovernanceType::getTypeWeight));
        //对各项指标加权计算求得每张表总分
        for (GovernanceAssessTable governanceAssessTable : governanceAssessTableList) {
            BigDecimal specScore = governanceAssessTable.getScoreSpecAvg().multiply(governanceTypeWeightMap.get(DgaConstant.GOVERNANCE_TYPE_SPEC));
            BigDecimal calcScore = governanceAssessTable.getScoreCalcAvg().multiply(governanceTypeWeightMap.get(DgaConstant.GOVERNANCE_TYPE_CALC));
            BigDecimal storageScore = governanceAssessTable.getScoreStorageAvg().multiply(governanceTypeWeightMap.get(DgaConstant.GOVERNANCE_TYPE_STORAGE));
            BigDecimal qualityScore = governanceAssessTable.getScoreQualityAvg().multiply(governanceTypeWeightMap.get(DgaConstant.GOVERNANCE_TYPE_QUALITY));
            BigDecimal securityScore = governanceAssessTable.getScoreSecurityAvg().multiply(governanceTypeWeightMap.get(DgaConstant.GOVERNANCE_TYPE_SECURITY));
            //加权计算总分
            BigDecimal totalScore = specScore.add(calcScore).add(storageScore).add(qualityScore).add(securityScore).divide(BigDecimal.TEN, 1, RoundingMode.HALF_UP);
            governanceAssessTable.setScoreOnTypeWeight(totalScore);
        }
        this.remove(new QueryWrapper<GovernanceAssessTable>().eq("assess_date", assessDate));
        this.saveBatch(governanceAssessTableList);
    }
}