package com.atguigu.dga;

import com.atguigu.dga.governance.service.GovernanceAssessDetailService;
import com.atguigu.dga.governance.service.GovernanceAssessGlobalService;
import com.atguigu.dga.governance.service.GovernanceAssessTableService;
import com.atguigu.dga.governance.service.GovernanceAssessTecOwnerService;
import com.atguigu.dga.meta.service.TableMetaInfoService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DgaApplicationTests {

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

    @Test
    void initTableMetaInfo() {
        tableMetaInfoService.initTableMetaInfo("gmall", "2023-05-02");
    }

    @Test
    void mainAccess() {
        governanceAssessDetailService.mainAccess("2023-05-02");
    }

    @Test
    void calcTableScore() {
        governanceAssessTableService.calcTableScore("2023-05-02");
    }

    @Test
    void calcTecOwnerScore() {
        governanceAssessTecOwnerService.calcTecOwnerScore("2023-05-02");
    }

    @Test
    void calcGlobalScore() {
        governanceAssessGlobalService.calcGlobalScore("2023-05-02");
    }
}
