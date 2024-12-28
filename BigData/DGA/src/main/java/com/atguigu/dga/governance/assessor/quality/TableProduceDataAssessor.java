package com.atguigu.dga.governance.assessor.quality;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Date;
import java.util.List;

@Component("TABLE_PRODUCE_DATA")
public class TableProduceDataAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws Exception {
        //筛选出日分区表
        //String lifecycleType = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleType();
        //if (!DgaConstant.LIFECYCLE_TYPE_DAY.equals(lifecycleType)) {
        //    return;
        //}
        //获取指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        Integer days = jsonObject.getInteger("days");
        Long upperLimit = jsonObject.getLong("upper_limit");
        Long lowerLimit = jsonObject.getLong("lower_limit");
        //获取分区字段
        String partitionColNameJson = assessParam.getTableMetaInfo().getPartitionColNameJson();
        List<JSONObject> jsonObjects = JSON.parseArray(partitionColNameJson, JSONObject.class);
        if (jsonObjects.isEmpty()) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("无分区字段");
            return;
        }
        String partitionCol = jsonObjects.get(0).getString("name");
        //获取FileSystem对象
        URI uri = new URI(assessParam.getTableMetaInfo().getTableFsPath());
        String owner = assessParam.getTableMetaInfo().getTableFsOwner();
        FileSystem fs = FileSystem.get(uri, new Configuration(), owner);
        //获取出当日日期
        Date assessDate = DateUtils.parseDate(assessParam.getAssessDate(), "yyyy-MM-dd");
        //遍历获取当日数据量和前days天数据量
        Long thisDataSize = 0L;
        Long beforeDataSize = 0L;
        Long beforeDays = 0L;
        Long avgDataSize;
        for (int i = -days; i < 0; ++i) {
            String dateString = DateFormatUtils.format(DateUtils.addDays(assessDate, i), "yyyy-MM-dd");
            String partitionPath = uri + "/" + partitionCol + "=" + dateString;
            //判断分区路径是否存在
            if (fs.exists(new Path(partitionPath))) {
                beforeDataSize += getPartitionDataSize(fs, partitionPath);
                beforeDays++;
                if (i == -1) {
                    thisDataSize += getPartitionDataSize(fs, partitionPath);
                }
            }
        }
        //若前days天均无数据
        if (beforeDataSize == 0) {
            governanceAssessDetail.setAssessComment("前" + days + "天无数据");
            return;
        } else {
            avgDataSize = beforeDataSize / beforeDays;
        }
        //计算当日数据与前days天数据的比值
        Long percent = 100L * (thisDataSize - avgDataSize) / avgDataSize + 100L;
        if (percent > 100L + upperLimit) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("当日产出数据量高");
            governanceAssessDetail.setAssessComment("当日产出数据量:" + thisDataSize + "，为前" + days + "天平均数据量" + avgDataSize + "的" + percent + "%");
        } else if (percent < lowerLimit) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("当日产出数据量低");
            governanceAssessDetail.setAssessComment("当日产出数据量:" + thisDataSize + "，为前" + days + "天平均数据量" + avgDataSize + "的" + percent + "%");
        }
    }

    private Long getPartitionDataSize(FileSystem fs, String path) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        Long size = 0L;
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                size += getPartitionDataSize(fs, fileStatus.getPath().toString());
            } else {
                size += fileStatus.getLen();
            }
        }
        return size;
    }
}
