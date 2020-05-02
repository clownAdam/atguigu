package com.atguigu.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author clown
 */
public class HdfsDriver extends Configuration implements Tool {
    Configuration configuration = null;

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);
        /*设置主类*/
        job.setJarByClass(HdfsDriver.class);
        /*设置mapper*/
        job.setMapperClass(HdfsMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);
        /*设置reducer*/
        TableMapReduceUtil.initTableReducerJob("fruit2", HdfsReducer.class, job);
        /*设置输入路径*/
        FileInputFormat.setInputPaths(job,args[0]);
        /*提交*/
        boolean wait = job.waitForCompletion(true);
        return wait ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int run = ToolRunner.run(conf, new HdfsDriver(), args);
        System.exit(run);
    }
}
