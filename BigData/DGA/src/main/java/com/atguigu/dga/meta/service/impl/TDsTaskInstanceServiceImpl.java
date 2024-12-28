package com.atguigu.dga.meta.service.impl;

import com.atguigu.dga.meta.bean.TDsTaskInstance;
import com.atguigu.dga.meta.mapper.TDsTaskInstanceMapper;
import com.atguigu.dga.meta.service.TDsTaskInstanceService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@DS("dolphinscheduler")
public class TDsTaskInstanceServiceImpl extends ServiceImpl<TDsTaskInstanceMapper, TDsTaskInstance> implements TDsTaskInstanceService {

    @Override
    public List<TDsTaskInstance> getTaskInstanceList(String assessDate) {
        List<TDsTaskInstance> taskInstanceList = this.list(
                new QueryWrapper<TDsTaskInstance>()
                        .exists(
                                "select\n" +
                                        "1\n" +
                                        "from\n" +
                                        "(\n" +
                                        "select\n" +
                                        "max(id) max_id\n" +
                                        "from\n" +
                                        "t_ds_task_instance\n" +
                                        "where\n" +
                                        "state = 7\n" +
                                        "and date_format(start_time, '%Y-%m-%d') = '" + assessDate + "'\n" +
                                        "group by\n" +
                                        "name\n" +
                                        ") tmax\n" +
                                        "where\n" +
                                        "t_ds_task_instance.id = tmax.max_id"
                        )
        );
        return taskInstanceList;
    }
}

