package com.atguigu.dga.meta.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.support.spring.PropertyPreFilters;
import com.atguigu.dga.meta.bean.TableMetaInfo;
import com.atguigu.dga.meta.bean.TableMetaInfoQuery;
import com.atguigu.dga.meta.bean.TableMetaInfoVO;
import com.atguigu.dga.meta.mapper.TableMetaInfoMapper;
import com.atguigu.dga.meta.service.TableMetaInfoExtraService;
import com.atguigu.dga.meta.service.TableMetaInfoService;
import com.atguigu.dga.util.SqlUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Service
public class TableMetaInfoServiceImpl extends ServiceImpl<TableMetaInfoMapper, TableMetaInfo> implements TableMetaInfoService {

    @Autowired
    TableMetaInfoExtraService tableMetaInfoExtraService;
    @Autowired
    TableMetaInfoMapper tableMetaInfoMapper;

    //获取hive metastore客户端
    //1. 创建hiveClient对象
    private IMetaStoreClient hiveClient;
    //2. 从properties中获取hiveMetaStoreUrl
    @Value("${hive.metastore.server.url}")
    public String hiveMetaStoreUrl;

    //3. hiveClient连接hivemetastore
    @PostConstruct
    public void createHiveMetaStoreClient() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getVarname(), hiveMetaStoreUrl);
        try {
            hiveClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initTableMetaInfo(String schemaName, String assessDate) {
        try {
            //获取hive表信息
            //1. 获取所有表名
            List<String> allTableName = hiveClient.getAllTables(schemaName);
            //2. 创建表元数据对象集合，用于存储
            List<TableMetaInfo> tableMetaInfos = new ArrayList<>(allTableName.size());
            //3. 依次获取每张表信息
            for (String tableName : allTableName) {
                Table table = hiveClient.getTable(schemaName, tableName);
                TableMetaInfo tableMetaInfo = new TableMetaInfo();
                extractTableMetaInfoFromHive(tableMetaInfo, table);
                extractTableMetaInfoFromHDFS(tableMetaInfo);
                tableMetaInfo.setAssessDate(assessDate);
                tableMetaInfo.setCreateTime(new Date());
                tableMetaInfos.add(tableMetaInfo);
            }
            //4. 清除今日已存在的数据，再将提取到的元数据信息存储到数据库中
            this.remove(new QueryWrapper<TableMetaInfo>().eq("assess_date", assessDate));
            this.saveBatch(tableMetaInfos);
            //5. 初始化辅助表的信息
            tableMetaInfoExtraService.initTableMetaInfoExtra(schemaName, allTableName);

            System.out.println("元数据初始化完毕");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //获取hive元数据信息
    private void extractTableMetaInfoFromHive(TableMetaInfo tableMetaInfo, Table table) {
        tableMetaInfo.setTableName(table.getTableName());
        tableMetaInfo.setSchemaName(table.getDbName());
        PropertyPreFilters.MySimplePropertyPreFilter columnFilter = new PropertyPreFilters().addFilter("comment", "name", "type");
        tableMetaInfo.setColNameJson(JSON.toJSONString(table.getSd().getCols(), columnFilter));
        tableMetaInfo.setPartitionColNameJson(JSON.toJSONString(table.getPartitionKeys()));
        tableMetaInfo.setTableFsOwner(table.getOwner());
        tableMetaInfo.setTableParametersJson(JSON.toJSONString(table.getParameters()));
        tableMetaInfo.setTableComment(table.getParameters().get("comment"));
        tableMetaInfo.setTableFsPath(table.getSd().getLocation());
        tableMetaInfo.setTableInputFormat(table.getSd().getInputFormat());
        tableMetaInfo.setTableOutputFormat(table.getSd().getOutputFormat());
        tableMetaInfo.setTableRowFormatSerde(table.getSd().getSerdeInfo().getSerializationLib());
        tableMetaInfo.setTableCreateTime(new Date(table.getCreateTime() * 1000L));
        tableMetaInfo.setTableType(table.getTableType());
        tableMetaInfo.setTableBucketColsJson(JSON.toJSONString(table.getSd().getBucketCols()));
        tableMetaInfo.setTableBucketNum((long) table.getSd().getNumBuckets());
        tableMetaInfo.setTableSortColsJson(JSON.toJSONString(table.getSd().getSortCols()));
    }

    //获取hdfs元数据信息
    private void extractTableMetaInfoFromHDFS(TableMetaInfo tableMetaInfo) throws Exception {
        FileSystem fs = FileSystem.get(new URI(tableMetaInfo.getTableFsPath()), new Configuration(), tableMetaInfo.getTableFsOwner());
        FileStatus[] fileStatuses = fs.listStatus(new Path(tableMetaInfo.getTableFsPath()));
        extractHDFSMetaInfo(fs, fileStatuses, tableMetaInfo);
        tableMetaInfo.setFsCapcitySize(fs.getStatus().getCapacity());
        tableMetaInfo.setFsUsedSize(fs.getStatus().getUsed());
        tableMetaInfo.setFsRemainSize(fs.getStatus().getRemaining());
    }

    //递归获取hdfs文件大小，最后修改与访问时间等信息
    private void extractHDFSMetaInfo(FileSystem fs, FileStatus[] fileStatuses, TableMetaInfo tableMetaInfo) throws Exception {
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                FileStatus[] subFileStatuses = fs.listStatus(fileStatus.getPath());
                extractHDFSMetaInfo(fs, subFileStatuses, tableMetaInfo);
            } else {
                tableMetaInfo.setTableSize(tableMetaInfo.getTableSize() == null ? 0 : tableMetaInfo.getTableSize() + fileStatus.getLen());
                tableMetaInfo.setTableTotalSize(tableMetaInfo.getTableTotalSize() == null ? 0 : tableMetaInfo.getTableTotalSize() + fileStatus.getLen() * fileStatus.getReplication());
                tableMetaInfo.setTableLastModifyTime(new Date(Math.max(tableMetaInfo.getTableLastModifyTime() == null ? 0L : tableMetaInfo.getTableLastModifyTime().getTime(), fileStatus.getModificationTime())));
                tableMetaInfo.setTableLastAccessTime(new Date(Math.max(tableMetaInfo.getTableLastAccessTime() == null ? 0L : tableMetaInfo.getTableLastAccessTime().getTime(), fileStatus.getAccessTime())));
            }
        }
    }

    //返回给Controller表信息列表
    @Override
    public List<TableMetaInfoVO> getTableMetaInfoVOList(TableMetaInfoQuery tableMetaInfoQuery) {
        StringBuilder sqlBuilder = new StringBuilder(
                "select\n" +
                        "count(*) over() as count,\n" +
                        "ti.id,\n" +
                        "ti.table_name,\n" +
                        "ti.schema_name,\n" +
                        "ti.table_size,\n" +
                        "ti.table_total_size,\n" +
                        "ti.table_comment,\n" +
                        "ti.table_last_modify_time,\n" +
                        "ti.table_last_access_time,\n" +
                        "te.tec_owner_user_name,\n" +
                        "te.busi_owner_user_name\n" +
                        "from\n" +
                        "table_meta_info ti\n" +
                        "join table_meta_info_extra te on ti.schema_name = te.schema_name\n" +
                        "and ti.table_name = te.table_name\n" +
                        "where\n" +
                        "ti.assess_date = (\n" +
                        "select\n" +
                        "max(assess_date)\n" +
                        "from\n" +
                        "table_meta_info\n" +
                        ")\n"
        );
        //库名过滤条件
        if (tableMetaInfoQuery.getSchemaName() != null && !tableMetaInfoQuery.getSchemaName().trim().isEmpty()) {
            sqlBuilder.append("and ti.schema_name = '")
                    .append(SqlUtil.filterUnsafeSql(tableMetaInfoQuery.getSchemaName()))
                    .append("'\n");
        }
        //表名过滤条件
        if (tableMetaInfoQuery.getTableName() != null && !tableMetaInfoQuery.getTableName().trim().isEmpty()) {
            sqlBuilder.append("and ti.table_name like '%")
                    .append(SqlUtil.filterUnsafeSql(tableMetaInfoQuery.getTableName()))
                    .append("%'\n");
        }
        //层级名过滤条件
        if (tableMetaInfoQuery.getDwLevel() != null && !tableMetaInfoQuery.getDwLevel().trim().isEmpty()) {
            sqlBuilder.append("and te.dw_level = '")
                    .append(SqlUtil.filterUnsafeSql(tableMetaInfoQuery.getDwLevel()))
                    .append("'\n");
        }
        //分页
        sqlBuilder.append("limit ")
                .append((tableMetaInfoQuery.getPageNo() - 1) * tableMetaInfoQuery.getPageSize())
                .append(",")
                .append(tableMetaInfoQuery.getPageSize());
        return tableMetaInfoMapper.excuteSelectSqlString(sqlBuilder.toString());
    }
}
