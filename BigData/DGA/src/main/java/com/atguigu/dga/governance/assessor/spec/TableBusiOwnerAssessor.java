package com.atguigu.dga.governance.assessor.spec;

import com.atguigu.dga.constant.DgaConstant;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("TABLE_BUSI_OWNER")
public class TableBusiOwnerAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) {
        //获取业务负责人
        String busiOwnerUserName = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getBusiOwnerUserName();
        if (busiOwnerUserName == null || busiOwnerUserName.trim().isEmpty() || DgaConstant.BUSI_OWNER_USER_NAME_UNSET.equals(busiOwnerUserName)) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("缺少业务负责人");
        }
    }
}
