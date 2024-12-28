package com.atguigu.dga.governance.mapper;

import com.atguigu.dga.governance.bean.GovernanceAssessTecOwner;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface GovernanceAssessTecOwnerMapper extends BaseMapper<GovernanceAssessTecOwner> {
    @Select(
            "select\n" +
                    "assess_date,\n" +
                    "tec_owner,\n" +
                    "avg(score_spec_avg)       as score_spec,\n" +
                    "avg(score_storage_avg)    as score_storage,\n" +
                    "avg(score_calc_avg)       as score_calc,\n" +
                    "avg(score_quality_avg)    as score_quality,\n" +
                    "avg(score_security_avg)   as score_security,\n" +
                    "avg(score_on_type_weight) as score,\n" +
                    "count(*)                  as table_num,\n" +
                    "sum(problem_num)          as problem_num,\n" +
                    "now()                     as create_time\n" +
                    "from\n" +
                    "governance_assess_table\n" +
                    "where\n" +
                    "assess_date = #{assessDate}\n" +
                    "group by\n" +
                    "assess_date,\n" +
                    "tec_owner"
    )
    List<GovernanceAssessTecOwner> selectGovernanceAssessTecOwnerList(@Param("assessDate") String assessDate);
}
