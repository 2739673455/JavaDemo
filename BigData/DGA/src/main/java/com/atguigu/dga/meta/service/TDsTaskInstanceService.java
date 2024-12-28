package com.atguigu.dga.meta.service;

import com.atguigu.dga.meta.bean.TDsTaskInstance;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 服务类
 * </p>
 *
 * @author -
 * @since 2024-10-03
 */
@DS("dolphinscheduler")
public interface TDsTaskInstanceService extends IService<TDsTaskInstance> {

    List<TDsTaskInstance> getTaskInstanceList(String assessDate);
}
