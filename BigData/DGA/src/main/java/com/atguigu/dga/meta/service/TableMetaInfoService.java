package com.atguigu.dga.meta.service;

import com.atguigu.dga.meta.bean.TableMetaInfo;
import com.atguigu.dga.meta.bean.TableMetaInfoQuery;
import com.atguigu.dga.meta.bean.TableMetaInfoVO;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;


public interface TableMetaInfoService extends IService<TableMetaInfo> {
    void initTableMetaInfo(String schemaName, String assessDate);

    List<TableMetaInfoVO> getTableMetaInfoVOList(TableMetaInfoQuery tableMetaInfoQuery);
}
