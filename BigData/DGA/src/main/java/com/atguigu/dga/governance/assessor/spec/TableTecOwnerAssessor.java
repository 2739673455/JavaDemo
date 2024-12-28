package com.atguigu.dga.governance.assessor.spec;

import com.atguigu.dga.constant.DgaConstant;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("TABLE_TEC_OWNER")
public class TableTecOwnerAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) {
        //获取技术负责人
        String tecOwnerUserName = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getTecOwnerUserName();
        if (tecOwnerUserName == null || tecOwnerUserName.trim().isEmpty() || DgaConstant.TEC_OWNER_USER_NAME_UNSET.equals(tecOwnerUserName)) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("缺少技术负责人");
        }
    }
}
