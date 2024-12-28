package com.atguigu.dga.governance.assessor.security;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dga.governance.assessor.Assessor;
import com.atguigu.dga.governance.bean.AssessParam;
import com.atguigu.dga.governance.bean.GovernanceAssessDetail;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

@Component("TABLE_DIR_FILE_PERMISSION")
public class TableDirFilePermissionAssessor extends Assessor {
    @Override
    public void checkProblem(AssessParam assessParam, GovernanceAssessDetail governanceAssessDetail) throws Exception {
        //获取指标参数
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        JSONObject jsonObject = JSON.parseObject(metricParamsJson);
        String filePermission = jsonObject.getString("file_permission");
        String dirPermission = jsonObject.getString("dir_permission");
        //获取FileSystem对象
        URI uri = new URI(assessParam.getTableMetaInfo().getTableFsPath());
        String owner = assessParam.getTableMetaInfo().getTableFsOwner();
        FileSystem fs = FileSystem.get(uri, new Configuration(), owner);
        //遍历所有文件，检查权限
        List<String> overPermissionList = new ArrayList<>();
        walk(fs, new Path(uri), overPermissionList, filePermission, dirPermission);

        if (!overPermissionList.isEmpty()) {
            governanceAssessDetail.setAssessScore(BigDecimal.ZERO);
            governanceAssessDetail.setAssessProblem("存在文件权限越界");
            governanceAssessDetail.setAssessComment("越界文件与目录：" + String.join("\n", overPermissionList));
        }
    }

    private void walk(FileSystem fs, Path path, List<String> overPermissionList, String filePermission, String dirPermission) throws IOException {
        if (fs.getFileStatus(path).isDirectory()) {
            checkPermission(fs, path, overPermissionList, dirPermission);
            FileStatus[] fileStatuses = fs.listStatus(path);
            for (FileStatus fileStatus : fileStatuses) {
                walk(fs, fileStatus.getPath(), overPermissionList, filePermission, dirPermission);
            }
        } else {
            checkPermission(fs, path, overPermissionList, filePermission);
        }
    }

    private void checkPermission(FileSystem fs, Path path, List<String> overPermissionList, String assessPermission) throws IOException {
        //两个权限数字进行或运算，结果减去标准权限数值，=0未越界，若>0，多的即为越界权限
        int assessUserPermission = assessPermission.charAt(0) - '0';
        int assessGroupPermission = assessPermission.charAt(1) - '0';
        int assessOtherPermission = assessPermission.charAt(2) - '0';
        int currentUserPermission = fs.getFileStatus(path).getPermission().getUserAction().ordinal();
        int currentGroupPermission = fs.getFileStatus(path).getPermission().getGroupAction().ordinal();
        int currentOtherPermission = fs.getFileStatus(path).getPermission().getOtherAction().ordinal();
        int userPermissionOver = (currentUserPermission | assessUserPermission) - assessUserPermission;
        int groupPermissionOver = (currentGroupPermission | assessGroupPermission) - assessGroupPermission;
        int otherPermissionOver = (currentOtherPermission | assessOtherPermission) - assessOtherPermission;

        if (userPermissionOver != 0 || groupPermissionOver != 0 || otherPermissionOver != 0) {
            overPermissionList.add(path.toString());
        }
    }
}
