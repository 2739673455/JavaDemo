package com.atguigu.dga.governance.assessor.spec;

import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.regex.Pattern;

@Component("TABLE_NAME_STANDARD")
public class TableNameStandardAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws ParseException {
        //获取表名，层级
        String tableName = assessParam.getTableMetaInfo().getTableName();
        String dwLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel();

        Pattern compile = null;
        switch (dwLevel) {
            case "ODS":
                compile = Pattern.compile("^ods_\\w+_(inc|full)$");
                break;
            case "DIM":
                compile = Pattern.compile("^dim_\\w+_(zip|full)$");
                break;
            case "DWD":
                compile = Pattern.compile("^dwd_\\w+_\\w+_(inc|full|acc)$");
                break;
            case "DWS":
                compile = Pattern.compile("^dws_\\w+_\\w+_\\w+_(1d|nd|td)$");
                break;
            case "ADS":
                compile = Pattern.compile("^ads_\\w+$");
                break;
            case "DM":
                compile = Pattern.compile("^dm_\\w+$");
                break;
            default:
                governanceAssessDetail.setAssessScore(BigDecimal.valueOf(5L));
                governanceAssessDetail.setAssessProblem("未纳入分层");
                return;
        }
        if (!compile.matcher(tableName).matches()) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("表名不合规");
        }
    }
}
