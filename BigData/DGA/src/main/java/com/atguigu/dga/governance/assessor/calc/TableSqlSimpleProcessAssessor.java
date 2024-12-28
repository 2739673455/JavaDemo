package com.atguigu.dga.governance.assessor.calc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.constant.DgaConstant;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.meta.bean.TDsTaskDefinition;
import com.atguigu.dga.meta.bean.TableMetaInfo;
import com.atguigu.dga.util.SqlUtil;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Sets;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.*;

@Component("TABLE_SQL_SIMPLE_PROCESS")
public class TableSqlSimpleProcessAssessor extends Assessor {
    @Value("${data.warehouse}")
    public String dataWarehouse;

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
        SqlDispatcher sqlDispatcher = new SqlDispatcher(dataWarehouse);
        SqlUtil.parseSql(sql, sqlDispatcher);
        //取出sql中包含的复杂计算放入集合
        Set<String> complexSet = sqlDispatcher.getComplexSet();
        //取出过滤字段和表名放入集合
        Set<String> filterColSet = sqlDispatcher.getFilterColSet();
        Set<String> tableFullNameSet = sqlDispatcher.getTableFullNameSet();
        //取出所有参与过滤的表中的分区字段
        Set<String> partitionCol = new HashSet<>();
        for (TableMetaInfo tableMetaInfo : assessParam.getTableMetaInfoList()) {
            if (tableFullNameSet.contains(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName())) {
                List<JSONObject> colJsonList = JSON.parseArray(tableMetaInfo.getPartitionColNameJson(), JSONObject.class);
                colJsonList.forEach(colJson -> partitionCol.add(colJson.getString("name")));
            }
        }
        //判断是否存在复杂计算，是否存在非分区的过滤条件
        Collection restColSet = CollectionUtils.subtract(filterColSet, partitionCol);
        if (!complexSet.isEmpty() || !restColSet.isEmpty()) {
            String assessComment = "";
            assessComment = complexSet.isEmpty() ? assessComment : assessComment + "存在复杂计算:" + complexSet + " ";
            assessComment = restColSet.isEmpty() ? assessComment : assessComment + "过滤字段存在非分区字段:" + restColSet;
            governanceAssessDetail.setAssessComment(assessComment);
        } else {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("无复杂计算，且where过滤字段皆为分区字段");
        }
    }

    public static class SqlDispatcher implements Dispatcher {
        private final String dataWarehouse;

        public SqlDispatcher(String dataWarehouse) {
            this.dataWarehouse = dataWarehouse;
        }

        //定义一个集合， 存储哪些计算是复杂计算
        Set<Integer> complicateTokSet = Sets.newHashSet(
                HiveParser.TOK_JOIN,   // join ,包含通过where连接的情况
                HiveParser.TOK_GROUPBY, // group by
                HiveParser.TOK_LEFTOUTERJOIN, // left join
                HiveParser.TOK_RIGHTOUTERJOIN, //right join
                HiveParser.TOK_FULLOUTERJOIN, // full join
                HiveParser.TOK_FUNCTION, // count(1)
                HiveParser.TOK_FUNCTIONDI, // count(distinct xx)
                HiveParser.TOK_FUNCTIONSTAR, // count(*)
                HiveParser.TOK_SELECTDI, // distinct
                HiveParser.TOK_UNIONALL // union
        );
        Set<Integer> operatorSet = com.google.common.collect.Sets.newHashSet(
                HiveParser.EQUAL,   // =
                HiveParser.GREATERTHAN, // >
                HiveParser.LESSTHAN, // <
                HiveParser.GREATERTHANOREQUALTO, // >=
                HiveParser.LESSTHANOREQUALTO, // <=
                HiveParser.NOTEQUAL, // <>
                HiveParser.KW_LIKE // like
        );

        @Getter
        Set<String> complexSet = new HashSet<>();
        @Getter
        Set<String> filterColSet = new HashSet<>();
        @Getter
        Set<String> tableFullNameSet = new HashSet<>();

        @Override
        public Object dispatch(Node node, Stack<Node> stack, Object... objects) throws SemanticException {
            ASTNode astNode = (ASTNode) node;
            //从sql中找出复杂计算
            if (complicateTokSet.contains(astNode.getType())) {
                complexSet.add(astNode.getText());
            }
            //从sql中找出是操作符，且祖先为TOK_WHERE的节点
            String operator = null;
            if (astNode.getAncestor(HiveParser.TOK_WHERE) != null && operatorSet.contains(astNode.getType())) {
                //获取其左节点，即为过滤条件的字段
                ASTNode leftNode = (ASTNode) astNode.getChild(0);
                //若为 . ，取其右节点字段；否则取其左节点
                if (leftNode.getType() == HiveParser.DOT)
                    operator = leftNode.getChild(1).getText();
                else if (leftNode.getType() == HiveParser.TOK_TABLE_OR_COL)
                    operator = leftNode.getChild(0).getText();
                if (operator != null) filterColSet.add(operator);
            }
            //从sql中找出是TOK_TABNAME 且祖先为TOK_FROM的节点
            String tableFullName;
            if (astNode.getAncestor(HiveParser.TOK_FROM) != null && astNode.getType() == HiveParser.TOK_TABNAME) {
                //若是 库名.表名 的形式，左节点为库名，右节点为表名；若是 表名 的形式，左节点为表名
                if (astNode.getChildren().size() == 2)
                    tableFullName = astNode.getChild(0).getText() + "." + astNode.getChild(1).getText();
                else
                    tableFullName = dataWarehouse + "." + astNode.getChild(0).getText();
                tableFullNameSet.add(tableFullName);
            }
            return null;
        }
    }
}
