package com.atguigu.dga.governance.mapper;

import com.atguigu.dga.governance.bean.GovernanceAssessGlobal;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * <p>
 * 治理总考评表 Mapper 接口
 * </p>
 *
 * @author -
 * @since 2024-10-08
 */
@Mapper
public interface GovernanceAssessGlobalMapper extends BaseMapper<GovernanceAssessGlobal> {
    @Select(
            "select\n" +
                    "assess_date,\n" +
                    "avg(score_spec_avg) * 10     as score_spec,\n" +
                    "avg(score_storage_avg) * 10  as score_storage,\n" +
                    "avg(score_calc_avg) * 10     as score_calc,\n" +
                    "avg(score_quality_avg) * 10  as score_quality,\n" +
                    "avg(score_security_avg) * 10 as score_security,\n" +
                    "avg(score_on_type_weight)    as score,\n" +
                    "count(*)                     as table_num,\n" +
                    "sum(problem_num)             as problem_num,\n" +
                    "now()                        as create_time\n" +
                    "from\n" +
                    "governance_assess_table\n" +
                    "where\n" +
                    "assess_date = #{assessDate}\n" +
                    "group by\n" +
                    "assess_date;"
    )
    GovernanceAssessGlobal selectGovernanceAssessGlobal(@Param("assessDate") String assessDate);
}
