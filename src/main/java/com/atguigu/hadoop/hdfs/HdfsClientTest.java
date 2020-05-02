package com.atguigu.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Administrator
 */
public class HdfsClientTest {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        /*1.获取hdfs客户端对象*/
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bd-101:9000");
        /*FileSystem fs = FileSystem.get(conf);*/
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "atguigu");
        /*2.在hdfs上创建路径*/
        fs.mkdirs(new Path("/0529/dashen/banzhang"));
        /*3.关闭资源*/
        fs.close();
        System.out.println("over");
    }

    /**
     * 1.文件上传
     */
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        /*1.获取fs对象*/
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "atguigu");
        /*2.执行上传api*/
        fs.copyFromLocalFile(new Path("input/banzhang.txt"), new Path("/banzhang.txt"));
        /*3.关闭资源*/
        fs.close();
    }

    /**
     * 2.文件下载
     */
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        /*1.获取fs对象*/
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "atguigu");
        /*2.执行下载操作*/
        /*fs.copyToLocalFile(new Path("/banhua.txt"),new Path("output/banhua.txt"));*/
        fs.copyToLocalFile(false, new Path("/banhua.txt"), new Path("output/banzhangs.txt"), true);
        /*3.关闭资源*/
        fs.close();
    }

    /**
     * 3.文件删除
     */
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        /*1.获取fs对象*/
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "atguigu");
        /*2.执行文件删除操作*/
        fs.delete(new Path("/user"), true);
        /*3.关闭资源*/
        fs.close();
    }

    /**
     * 4.文件名更改
     */
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        /*1.获取fs对象*/
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "atguigu");
        /*2.执行更名操作*/
        fs.rename(new Path("/banzhang.txt"), new Path("/yanjing.txt"));
        /*3.关闭资源*/
        fs.close();
    }

    /**
     * 5.文件详情查看
     */
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        /*1.获取文件系统*/
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "atguigu");
        /*2.获取文件详情*/
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("fileStatus = " + fileStatus);
            /*查看文件名称，权限，长度，块信息*/
            String name = fileStatus.getPath().getName();
            System.out.println("name = " + name);
            FsPermission permission = fileStatus.getPermission();
            System.out.println("permission = " + permission);
            long len = fileStatus.getLen();
            System.out.println("len = " + len);
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-----------------");
        }
        /*3.关闭资源*/
        fs.close();
    }

    /**
     * 6.判断是文件还是文件夹
     */
    @Test
    public void testListStatus() throws Exception {
        /*1.获取fs对象*/
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "user");
        /*2.判断操作*/
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isFile()) {
                /*文件*/
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                /*文件夹*/
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        /*3.关闭资源*/
        fs.close();
    }
}
