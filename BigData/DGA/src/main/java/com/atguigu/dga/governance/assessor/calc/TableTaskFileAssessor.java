package com.atguigu.dga.governance.assessor.calc;

import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.meta.bean.TDsTaskInstance;
import com.atguigu.dga.meta.service.TDsTaskInstanceService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Component("TABLE_TASK_FAIL")
public class TableTaskFileAssessor extends Assessor {
    @Autowired
    TDsTaskInstanceService tDsTaskInstanceService;

    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws Exception {
        //获取当日该表任务失败实例
        List<TDsTaskInstance> failTaskList = tDsTaskInstanceService.list(
                new QueryWrapper<TDsTaskInstance>()
                        .eq("name", assessParam.getTableMetaInfo().getSchemaName() + "." + assessParam.getTableMetaInfo().getTableName())
                        .eq("date_format(start_time,'%Y-%m-%d')", assessParam.getAssessDate())
                        .eq("state", 6)
        );
        if (!failTaskList.isEmpty()) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("计算报错");
            List<Integer> failTaskIdList = failTaskList.stream().map(TDsTaskInstance::getId).collect(Collectors.toList());
            governanceAssessDetail.setAssessComment("报错任务id:" + failTaskIdList);
        }
    }
}
