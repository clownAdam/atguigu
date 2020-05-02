package com.atguigu.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @author clown
 * 循环遍历文件和文件夹
 */
public class Demo {
    public static StringBuffer buffer = new StringBuffer();
    public static Configuration conf = new Configuration();
    public static FileSystem fs;

    static {
        try {
            fs = FileSystem.get(new URI("hdfs://bd-101:9000"), conf, "user");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void close() throws IOException {
        if(fs!=null){
            fs.close();
        }
    }
    public static void main(String[] args) throws Exception {
        listStatus("/");
        close();
    }

    private static void listStatus(String path) throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path(path));
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isFile()) {
                /*文件*/
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                /*文件夹*/
                System.out.println("d:" + fileStatus.getPath().getName());
                String name = fileStatus.getPath().getName();
                buffer.append("/"+name);
                listStatus(buffer.toString());
            }
        }
    }
}
