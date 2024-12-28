package com.atguigu.dga.meta.service.impl;

import com.atguigu.dga.constant.DgaConstant;
import com.atguigu.dga.meta.bean.TableMetaInfoExtra;
import com.atguigu.dga.meta.mapper.TableMetaInfoExtraMapper;
import com.atguigu.dga.meta.service.TableMetaInfoExtraService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Service
public class TableMetaInfoExtraServiceImpl extends ServiceImpl<TableMetaInfoExtraMapper, TableMetaInfoExtra> implements TableMetaInfoExtraService {
    //初始化辅助数据
    @Override
    public void initTableMetaInfoExtra(String schemaName, List<String> tableNames) {
        ArrayList<TableMetaInfoExtra> tableMetaInfoExtras = new ArrayList<>(tableNames.size());
        for (String tableName : tableNames) {
            TableMetaInfoExtra tableMetaInfoExtra = this.getOne(new QueryWrapper<TableMetaInfoExtra>()
                    .eq("schema_name", schemaName)
                    .eq("table_name", tableName));
            if (tableMetaInfoExtra == null) {
                tableMetaInfoExtra = new TableMetaInfoExtra();
                tableMetaInfoExtra.setTableName(tableName);
                tableMetaInfoExtra.setSchemaName(schemaName);
                tableMetaInfoExtra.setTecOwnerUserName(DgaConstant.TEC_OWNER_USER_NAME_UNSET);
                tableMetaInfoExtra.setBusiOwnerUserName(DgaConstant.BUSI_OWNER_USER_NAME_UNSET);
                tableMetaInfoExtra.setLifecycleType(DgaConstant.LIFECYCLE_TYPE_UNSET);
                tableMetaInfoExtra.setLifecycleDays(-1L);
                tableMetaInfoExtra.setSecurityLevel(DgaConstant.SECURITY_LEVEL_UNSET);
                tableMetaInfoExtra.setDwLevel(getTableLevelByName(tableName));
                tableMetaInfoExtra.setCreateTime(new Date());
                tableMetaInfoExtras.add(tableMetaInfoExtra);
            }
        }
        this.saveBatch(tableMetaInfoExtras);
    }

    //设置表层级
    private String getTableLevelByName(String tableName) {
        if (tableName.startsWith("ods")) {
            return DgaConstant.DW_LEVEL_ODS;
        } else if (tableName.startsWith("dwd")) {
            return DgaConstant.DW_LEVEL_DWD;
        } else if (tableName.startsWith("dim")) {
            return DgaConstant.DW_LEVEL_DIM;
        } else if (tableName.startsWith("dws")) {
            return DgaConstant.DW_LEVEL_DWS;
        } else if (tableName.startsWith("ads")) {
            return DgaConstant.DW_LEVEL_ADS;
        } else if (tableName.startsWith("dm")) {
            return DgaConstant.DW_LEVEL_DM;
        } else {
            return DgaConstant.DW_LEVEL_OTHER;
        }
    }
}
