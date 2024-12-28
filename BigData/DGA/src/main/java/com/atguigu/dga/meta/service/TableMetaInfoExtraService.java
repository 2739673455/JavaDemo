package com.atguigu.dga.meta.service;

import com.atguigu.dga.meta.bean.TableMetaInfoExtra;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 元数据表附加信息 服务类
 * </p>
 *
 * @author -
 * @since 2024-09-25
 */
public interface TableMetaInfoExtraService extends IService<TableMetaInfoExtra> {
    void initTableMetaInfoExtra(String schemaName, List<String> tableNames);
}
