package com.atguigu.dga.meta.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.meta.bean.TDsTaskDefinition;
import com.atguigu.dga.meta.mapper.TDsTaskDefinitionMapper;
import com.atguigu.dga.meta.service.TDsTaskDefinitionService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author -
 * @since 2024-10-03
 */
@Service
@DS("dolphinscheduler")
public class TDsTaskDefinitionServiceImpl extends ServiceImpl<TDsTaskDefinitionMapper, TDsTaskDefinition> implements TDsTaskDefinitionService {

    @Override
    public List<TDsTaskDefinition> getTaskDefinitionList() {
        //筛选shell任务
        List<TDsTaskDefinition> tDsTaskDefinitionList = this.list(
                new QueryWrapper<TDsTaskDefinition>().eq("task_type", "SHELL")
        );
        tDsTaskDefinitionList.forEach(this::extractSQL);
        return tDsTaskDefinitionList;
    }

    private void extractSQL(TDsTaskDefinition tDsTaskDefinition) {
        //提取任务定义中的rawScript
        String taskParams = tDsTaskDefinition.getTaskParams();
        JSONObject jsonObject = JSON.parseObject(taskParams);
        String rawScript = jsonObject.getString("rawScript");
        //从rawScript中提取sql语句
        String regex = "((with.*)?insert.+?)[;\"]";
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(rawScript);
        while (matcher.find()) {
            String sqlString = matcher.group(1).trim();
            tDsTaskDefinition.setTaskSql(sqlString);
        }
    }
}
