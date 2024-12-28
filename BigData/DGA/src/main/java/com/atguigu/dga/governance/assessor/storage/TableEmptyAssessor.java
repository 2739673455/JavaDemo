package com.atguigu.dga.governance.assessor.storage;

import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component("TABLE_EMPTY")
public class TableEmptyAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) {
        //获取文件大小
        Long tableTotalSize = assessParam.getTableMetaInfo().getTableTotalSize();
        if (tableTotalSize == null || tableTotalSize <= 0) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("空表");
        }
    }
}
