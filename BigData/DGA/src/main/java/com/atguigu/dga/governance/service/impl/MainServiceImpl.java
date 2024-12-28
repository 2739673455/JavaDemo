package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.governance.service.*;
import com.atguigu.dga.meta.service.TableMetaInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class MainServiceImpl implements MainService {

    @Value("${data.warehouse}")
    private String dataWarehouse;

    @Autowired
    TableMetaInfoService tableMetaInfoService;

    @Autowired
    GovernanceAssessDetailService governanceAssessDetailService;

    @Autowired
    GovernanceAssessTableService governanceAssessTableService;

    @Autowired
    GovernanceAssessTecOwnerService governanceAssessTecOwnerService;

    @Autowired
    GovernanceAssessGlobalService governanceAssessGlobalService;

    @Override
    public void startGovernanceAssess(String schemaName, String assessDate) {
        tableMetaInfoService.initTableMetaInfo(schemaName, assessDate);
        governanceAssessDetailService.mainAccess(assessDate);
        governanceAssessTableService.calcTableScore(assessDate);
        governanceAssessTecOwnerService.calcTecOwnerScore(assessDate);
        governanceAssessGlobalService.calcGlobalScore(assessDate);

        System.out.println("结果统计完毕");
    }

    @Override
    public void startGovernanceAssess(String assessDate) {
        startGovernanceAssess(dataWarehouse, assessDate);
    }

    @Scheduled(cron = "00 29 11 * * *")
    @Override
    public void startGovernanceAssess() {
        //String assessDate = DateFormatUtils.format(new Date(), "yyyy-MM-dd");
        String assessDate = "2023-05-02";
        startGovernanceAssess(dataWarehouse, assessDate);
    }
}
