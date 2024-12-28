package com.atguigu.dga.governance.bean;

import com.atguigu.dga.meta.bean.TDsTaskDefinition;
import com.atguigu.dga.meta.bean.TDsTaskInstance;
import com.atguigu.dga.meta.bean.TableMetaInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AssessParam {
    private TableMetaInfo tableMetaInfo;
    private GovernanceMetric governanceMetric;
    private String assessDate;
    private List<TableMetaInfo> tableMetaInfoList;
    private TDsTaskDefinition tdsTaskDefinition;
    private TDsTaskInstance tdsTaskInstance;
}
