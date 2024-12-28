package com.atguigu.dga.meta.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.meta.bean.TableMetaInfo;
import com.atguigu.dga.meta.bean.TableMetaInfoExtra;
import com.atguigu.dga.meta.bean.TableMetaInfoQuery;
import com.atguigu.dga.meta.bean.TableMetaInfoVO;
import com.atguigu.dga.meta.service.TableMetaInfoExtraService;
import com.atguigu.dga.meta.service.TableMetaInfoService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@RestController
@RequestMapping("/tableMetaInfo")
public class TableMetaInfoController {

    @Autowired
    TableMetaInfoService tableMetaInfoService;
    @Autowired
    TableMetaInfoExtraService tableMetaInfoExtraService;

    //表信息列表与表数量
    @GetMapping("/table-list")
    public String tableList(TableMetaInfoQuery tableMetaInfoQuery) {
        List<TableMetaInfoVO> tableMetaInfoVOList = tableMetaInfoService.getTableMetaInfoVOList(tableMetaInfoQuery);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("list", tableMetaInfoVOList);
        jsonObject.put("total", tableMetaInfoVOList.isEmpty() ? 0 : tableMetaInfoVOList.get(0).getCount());
        return jsonObject.toJSONString();
    }

    //单表详情
    @GetMapping("/table/{tableMetaInfoId}")
    public String table(@PathVariable("tableMetaInfoId") Long tableMetaInfoId) {
        TableMetaInfo tableMetaInfo = tableMetaInfoService.getOne(
                new QueryWrapper<TableMetaInfo>()
                        .eq("id", tableMetaInfoId)
                        .inSql("assess_date", "select max(assess_date) from table_meta_info")
        );
        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfoExtraService.getOne(
                new QueryWrapper<TableMetaInfoExtra>()
                        .eq("schema_name", tableMetaInfo.getSchemaName())
                        .eq("table_name", tableMetaInfo.getTableName())
        );
        tableMetaInfo.setTableMetaInfoExtra(tableMetaInfoExtra);
        return JSON.toJSONString(tableMetaInfo);
    }

    //辅助信息修改
    @PostMapping("/tableExtra")
    public String tableExtra(@RequestBody TableMetaInfoExtra tableMetaInfoExtra) {
        tableMetaInfoExtraService.saveOrUpdate(tableMetaInfoExtra);
        return "success";
    }

    //提取元数据
    @PostMapping("/init_tables/{schemaName}/{assessDate}")
    public String initTables(@PathVariable("schemaName") String schemaName, @PathVariable("assessDate") String assessDate) {
        tableMetaInfoService.initTableMetaInfo(schemaName, assessDate);
        return "success";
    }
}
