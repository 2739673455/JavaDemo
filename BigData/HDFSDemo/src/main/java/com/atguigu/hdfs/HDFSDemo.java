package com.atguigu.hdfs;

/*
    通过代码操作HDFS
    1.创建对象
    2.具体操作
    3.关闭资源
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class HDFSDemo {
    private FileSystem fs;


    @Before
    public void before() throws Exception {
        URI uri = new URI("hdfs://hadoop102:8020");
        Configuration conf = new Configuration();
        String user = "atguigu";
        fs = FileSystem.get(uri, conf, user);
    }

    @After
    public void after() throws Exception {
        if (fs != null) {
            fs.close();
        }
    }

    @Test
    public void test1() throws Exception {
        //上传
        fs.copyFromLocalFile(
                false,
                true,
                new Path("D:/hello.txt"),
                new Path("/input")
        );
    }

    @Test
    public void test2() throws Exception {
        //下载
        fs.copyToLocalFile(
                false,
                new Path("/input/hello.txt"),
                new Path("D:"),
                false
        );
    }
}