package com.atguigu.dga.governance.mapper;

import com.atguigu.dga.governance.bean.GovernanceAssessTable;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@DS("dga")
@Mapper
public interface GovernanceAssessTableMapper extends BaseMapper<GovernanceAssessTable> {
    @Select(
            "select\n" +
                    "assess_date,\n" +
                    "table_name,\n" +
                    "schema_name,\n" +
                    "tec_owner,\n" +
                    "avg(if(governance_type = 'SPEC', assess_score, null))     as score_spec_avg,\n" +
                    "avg(if(governance_type = 'STORAGE', assess_score, null))  as score_storage_avg,\n" +
                    "avg(if(governance_type = 'CALC', assess_score, null))     as score_calc_avg,\n" +
                    "avg(if(governance_type = 'QUALITY', assess_score, null))  as score_quality_avg,\n" +
                    "avg(if(governance_type = 'SECURITY', assess_score, null)) as score_security_avg,\n" +
                    "sum(if(assess_score < 10, 1, 0))                          as problem_num,\n" +
                    "now()                                                     as create_time\n" +
                    "from\n" +
                    "governance_assess_detail\n" +
                    "where\n" +
                    "assess_date = #{assessDate}\n" +
                    "group by\n" +
                    "assess_date,\n" +
                    "table_name,\n" +
                    "schema_name,\n" +
                    "tec_owner"
    )
    List<GovernanceAssessTable> selectGovernanceAssessTableList(@Param("assessDate") String assessDate);
}
