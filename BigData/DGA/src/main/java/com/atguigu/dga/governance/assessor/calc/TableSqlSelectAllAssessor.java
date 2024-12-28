package com.atguigu.dga.governance.assessor.calc;

import com.atguigu.dga.constant.DgaConstant;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.meta.bean.TDsTaskDefinition;
import com.atguigu.dga.util.SqlUtil;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Stack;

@Component("TABLE_SQL_SELECT_ALL")
public class TableSqlSelectAllAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws Exception {
        //排除ods表
        if (DgaConstant.DW_LEVEL_ODS.equals(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel()))
            return;
        //获取sql语句
        TDsTaskDefinition tdsTaskDefinition = assessParam.getTdsTaskDefinition();
        if (tdsTaskDefinition == null) return;
        String sql = tdsTaskDefinition.getTaskSql();
        if (sql == null) return;
        //使用sql解析工具解析sql
        SqlDispatcher sqlDispatcher = new SqlDispatcher();
        SqlUtil.parseSql(sql, sqlDispatcher);

        if (sqlDispatcher.flag) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("包含 select *");
        }
    }

    //创建sql解析工具
    public static class SqlDispatcher implements Dispatcher {
        //每遍历一个节点，都调用该方法对节点处理
        @Override
        public Object dispatch(Node node, Stack<Node> stack, Object... objects) throws SemanticException {
            //判断当前节点是否为 select * 类型
            ASTNode astNode = (ASTNode) node;
            if (astNode.getType() == HiveParser.TOK_ALLCOLREF) {
                flag = true;
            }
            return null;
        }

        public Boolean flag = false;
    }
}
