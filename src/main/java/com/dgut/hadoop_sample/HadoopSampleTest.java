package com.dgut.hadoop_sample;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 参数优先级排序：
 * （1）客户端代码中设置的值
 * （2）ClassPath 下的用户自定义配置 文件
 * （3）然后是服务器的默认配置
 */


/**
 * @author linwt
 * @date 2019/11/26 9:54
 */
public class HadoopSampleTest {
    @Test
    public void mkdir() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://centos-l1.niracler.com:9000");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://centos-l1.niracler.com:9000"), configuration, "enda");
        fileSystem.mkdirs(new Path("/test/fs"));
        fileSystem.close();
    }

    @Test
    public void copyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-l1.niracler.com:9000"), configuration, "enda");

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/test/test.txt"), new Path("./fs.txt"), true);

        fs.close();
    }

    @Test
    public void delete() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-l1.niracler.com:9000"), configuration, "enda");

        // 执行删除
        fs.delete(new Path("/test/fs"), true);
        fs.close();
    }

    @Test
    public void rename() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-l1.niracler.com:9000"), configuration, "enda");

        fs.rename(new Path("/test/test.txt"), new Path("/test/hello.txt"));
        fs.close();
    }

    @Test
    public void read() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos-l1.niracler.com:9000"), configuration, "enda");

        FSDataInputStream inputStream = fs.open(new Path("/test/hello.txt"));
        byte[] buf = new byte[1024];
        inputStream.read(buf);
        System.out.println(new String(buf));
        inputStream.close();
        fs.close();
    }
}
