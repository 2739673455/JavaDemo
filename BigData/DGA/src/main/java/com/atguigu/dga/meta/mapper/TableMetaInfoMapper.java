package com.atguigu.dga.meta.mapper;

import com.atguigu.dga.meta.bean.TableMetaInfo;
import com.atguigu.dga.meta.bean.TableMetaInfoVO;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 * 元数据表 Mapper 接口
 * </p>
 *
 * @author -
 * @since 2024-09-25
 */
@Mapper
public interface TableMetaInfoMapper extends BaseMapper<TableMetaInfo> {

    @Select("${sql}")
    List<TableMetaInfoVO> excuteSelectSqlString(@Param("sql") String sql);

    @Select("select\n" +
            "ti.id                      as ti_id,\n" +
            "ti.table_name              as ti_table_name,\n" +
            "ti.schema_name             as ti_schema_name,\n" +
            "ti.col_name_json,\n" +
            "ti.partition_col_name_json,\n" +
            "ti.table_fs_owner,\n" +
            "ti.table_parameters_json,\n" +
            "ti.table_comment,\n" +
            "ti.table_fs_path,\n" +
            "ti.table_input_format,\n" +
            "ti.table_output_format,\n" +
            "ti.table_row_format_serde,\n" +
            "ti.table_create_time,\n" +
            "ti.table_type,\n" +
            "ti.table_bucket_cols_json,\n" +
            "ti.table_bucket_num,\n" +
            "ti.table_sort_cols_json,\n" +
            "ti.table_size,\n" +
            "ti.table_total_size,\n" +
            "ti.table_last_modify_time,\n" +
            "ti.table_last_access_time,\n" +
            "ti.fs_capcity_size,\n" +
            "ti.fs_used_size,\n" +
            "ti.fs_remain_size,\n" +
            "ti.assess_date,\n" +
            "ti.create_time             as ti_create_time,\n" +
            "ti.update_time             as ti_update_time,\n" +
            "te.id                      as te_id,\n" +
            "te.table_name              as te_table_name,\n" +
            "te.schema_name             as te_schema_name,\n" +
            "te.tec_owner_user_name,\n" +
            "te.busi_owner_user_name,\n" +
            "te.lifecycle_type,\n" +
            "te.lifecycle_days,\n" +
            "te.security_level,\n" +
            "te.dw_level,\n" +
            "te.create_time             as te_create_time,\n" +
            "te.update_time             as te_update_time\n" +
            "from\n" +
            "table_meta_info ti\n" +
            "join table_meta_info_extra te on ti.schema_name = te.schema_name\n" +
            "and ti.table_name = te.table_name\n" +
            "where\n" +
            "ti.assess_date = #{assessDate}")
    @ResultMap("table_meta_info_with_extra_result_map")
    List<TableMetaInfo> selectAllTableMetaInfoWithExtra(@Param("assessDate") String assessDate);
}
