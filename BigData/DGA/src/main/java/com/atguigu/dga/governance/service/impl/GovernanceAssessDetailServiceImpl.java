package com.atguigu.dga.governance.service.impl;

import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import com.atguigu.dga.governance.bean.GovernanceMetric;
import com.atguigu.dga.governance.mapper.GovernanceAssessDetailMapper;
import com.atguigu.dga.governance.service.GovernanceAssessDetailService;
import com.atguigu.dga.governance.service.GovernanceMetricService;
import com.atguigu.dga.meta.bean.TDsTaskDefinition;
import com.atguigu.dga.meta.bean.TDsTaskInstance;
import com.atguigu.dga.meta.bean.TableMetaInfo;
import com.atguigu.dga.meta.mapper.TableMetaInfoMapper;
import com.atguigu.dga.meta.service.TDsTaskDefinitionService;
import com.atguigu.dga.meta.service.TDsTaskInstanceService;
import com.atguigu.dga.meta.service.TableMetaInfoExtraService;
import com.atguigu.dga.meta.service.TableMetaInfoService;
import com.atguigu.dga.util.SpringBeanProvider;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class GovernanceAssessDetailServiceImpl extends ServiceImpl<GovernanceAssessDetailMapper, GovernanceAssessDetail> implements GovernanceAssessDetailService {
    @Autowired
    GovernanceMetricService governanceMetricService;
    @Autowired
    TableMetaInfoService tableMetaInfoService;
    @Autowired
    TableMetaInfoExtraService tableMetaInfoExtraService;
    @Autowired
    TableMetaInfoMapper tableMetaInfoMapper;
    @Autowired
    SpringBeanProvider springBeanProvider;
    @Autowired
    TDsTaskDefinitionService tDsTaskDefinitionService;
    @Autowired
    TDsTaskInstanceService tDsTaskInstanceService;

    @Override
    public void mainAccess(String assessDate) {
        //1. 查询所有被考评的表
        //  方式一: Map
        //List<TableMetaInfo> tableMetaInfoList = tableMetaInfoService.list(new QueryWrapper<TableMetaInfo>().eq("assess_date", assessDate));
        //List<TableMetaInfoExtra> tableMetaInfoExtraList = tableMetaInfoExtraService.list();
        //Map<String, TableMetaInfoExtra> tableMetaInfoMap = new HashMap<>();
        //tableMetaInfoExtraList.forEach(tableMetaInfoExtra -> tableMetaInfoMap.put(tableMetaInfoExtra.getSchemaName() + tableMetaInfoExtra.getTableName(), tableMetaInfoExtra));
        //tableMetaInfoList.forEach(tableMetaInfo -> tableMetaInfo.setTableMetaInfoExtra(tableMetaInfoMap.get(tableMetaInfo.getSchemaName() + tableMetaInfo.getTableName())));
        //  方式二: 基于Join的方式，配合MyBatis中的自定义结果集映射(resultMap) ，一次性将所要的数据从数据库表中查询出来
        List<TableMetaInfo> tableMetaInfoList = tableMetaInfoMapper.selectAllTableMetaInfoWithExtra(assessDate);
        //2. 查询所有指标，任务定义，任务实例
        List<GovernanceMetric> governanceMetricList = governanceMetricService.list(
                new QueryWrapper<GovernanceMetric>().eq("is_disabled", 0)
        );
        List<TDsTaskDefinition> taskDefinitionList = tDsTaskDefinitionService.getTaskDefinitionList();
        Map<String, TDsTaskDefinition> tDsTaskDefinitionMap = new HashMap<>(taskDefinitionList.size());
        taskDefinitionList.forEach(o -> tDsTaskDefinitionMap.put(o.getName(), o));
        List<TDsTaskInstance> taskInstanceList = tDsTaskInstanceService.getTaskInstanceList(assessDate);
        Map<String, TDsTaskInstance> tDsTaskInstanceMap = new HashMap<>(taskInstanceList.size());
        taskInstanceList.forEach(o -> tDsTaskInstanceMap.put(o.getName(), o));
        //3. 每张表每个指标逐一考评
        //  创建集合存储考评结果
        List<GovernanceAssessDetail> governanceAssessDetailList = new ArrayList<>(tableMetaInfoList.size() * governanceMetricList.size());

        //多线程：创建线程池，创建集合维护任务
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                20,
                20,
                10,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(1500));
        List<CompletableFuture<GovernanceAssessDetail>> futureList = new ArrayList<>(tableMetaInfoList.size() * governanceMetricList.size());

        for (TableMetaInfo tableMetaInfo : tableMetaInfoList) {
            for (GovernanceMetric governanceMetric : governanceMetricList) {
                //白名单
                String skipAssessTables = governanceMetric.getSkipAssessTables();
                if (skipAssessTables != null && !skipAssessTables.trim().isEmpty()) {
                    List<String> skipTableList = Arrays.asList(skipAssessTables.split(","));
                    if (skipTableList.contains(tableMetaInfo.getTableName())) {
                        continue;
                    }
                }
                //获取考评器对象,方式一: 反射
                //  要求考评器的类名为必须使用指标的编码来命名
                //  要求考评器所在的包必须是指标的类型，且在同一个basePackage中
                //  按照指标信息处理出对应考评器的全类名
                //String basePackage = "com.atguigu.dga.governance.assessor";
                //String subPackage = governanceMetric.getGovernanceType().toLowerCase();
                //String className = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, governanceMetric.getMetricCode()) + "Assessor";
                //String fullClassName = basePackage + "." + subPackage + "." + className;
                //Assessor assessor = (Assessor) Class.forName(fullClassName).newInstance();
                //获取考评器对象,方式二: Spring容器
                Assessor assessor = springBeanProvider.getBean(governanceMetric.getMetricCode(), Assessor.class);
                //获取考评参数
                AssessParam assessParam = AssessParam.builder()
                        .tableMetaInfo(tableMetaInfo)
                        .governanceMetric(governanceMetric)
                        .assessDate(assessDate)
                        .tableMetaInfoList(tableMetaInfoList)
                        .tdsTaskDefinition(tDsTaskDefinitionMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName()))
                        .tdsTaskInstance(tDsTaskInstanceMap.get(tableMetaInfo.getSchemaName() + "." + tableMetaInfo.getTableName()))
                        .build();
                //进行考评，将考评结果放入集合
                //governanceAssessDetailList.add(assessor.doAssess(assessParam));

                //多线程：将考评过程处理成任务
                CompletableFuture<GovernanceAssessDetail> future = CompletableFuture.supplyAsync(
                        () -> assessor.doAssess(assessParam),
                        threadPool
                );
                futureList.add(future);
            }
        }

        //多线程：执行任务并收集结果
        governanceAssessDetailList = futureList.stream().map(CompletableFuture::join).collect(Collectors.toList());

        //4. 存储考评结果
        this.remove(new QueryWrapper<GovernanceAssessDetail>().eq("assess_date", assessDate));
        this.saveBatch(governanceAssessDetailList);

        System.out.println("考评完毕");
    }
}
