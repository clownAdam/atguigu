package com.atguigu.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author clown
 */
public class HdfsIoTest {
    /**
     * 将本地文件上传到hdfs目录
     */
    @Test
    public void putFileToHdfs() throws Exception {
        /*1.获取fs对象*/
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "atguigu");
        /*2.获取输入流*/
        FileInputStream fis = new FileInputStream(new File("input/banzhang.txt"));
        /*3.获取输出流*/
        FSDataOutputStream fos = fs.create(new Path("/banzhang.txt"));
        /*4.流的对拷*/
        IOUtils.copyBytes(fis, fos, conf);
        /*5.关闭资源*/
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 将hdfs文件下载到本地
     */
    @Test
    public void getFileFromHdfs() throws Exception {
        /*1.获取fs对象*/
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "atguigu");
        /*2.获取hdfs输入流*/
        FSDataInputStream fis = fs.open(new Path("/banhua.txt"));
        /*3.获取本地输出流*/
        FileOutputStream fos = new FileOutputStream("output/banhua111.txt");
        /*4.流的对拷*/
        IOUtils.copyBytes(fis, fos, conf);
        /*5.关闭资源*/
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 分块读取hdfs上的大文件到本地-->第一块
     */
    @Test
    public void readFileSeek1() throws Exception {
        /*1.获取fs对象*/
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "atguigu");
        /*2.获取输入流*/
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        /*3.获取输出流*/
        FileOutputStream fos = new FileOutputStream(new File("D:/tmp/hadoop-2.7.2.tar.gz.part1"));
        /*4.流的对拷(只拷贝128M)*/
        byte[] buf = new byte[1024];
        for (int i = 0, length = 1024 * 128; i < length; i++) {
            fis.read(buf);
            fos.write(buf);
        }
        /*5.关闭资源*/
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 分块读取hdfs上的大文件到本地-->第二块
     */
    @Test
    public void readFileSeek2() throws Exception {
        /*1.获取fs对象*/
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "atguigu");
        /*2.获取输入流*/
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        /*3.设定指定读取的起点*/
        fis.seek(1024*1024*128);
        /*4.获取输出流*/
        FileOutputStream fos = new FileOutputStream(new File("D:/tmp/hadoop-2.7.2.tar.gz.part2"));
        /*5.流的对拷*/
        IOUtils.copyBytes(fis, fos,conf);
        /*6.关闭资源*/
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
