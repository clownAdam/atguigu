package com.atguigu.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author clown
 */
public class FruitDriver extends Configuration implements Tool {
    private Configuration configuration = null;

    /**
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        /*获取任务对象*/
        Job job = Job.getInstance(configuration);
        /*指定driver类*/
        job.setJarByClass(FruitDriver.class);
        /*指定mapper-->hbase读取*/
        TableMapReduceUtil.initTableMapperJob(
                "fruit",
                new Scan(),
                FruitMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);
        /*指定reducer*/
        TableMapReduceUtil.initTableReducerJob("fruit_mr", FruitReducer.class, job);
        /*提交*/
        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
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
        int run = ToolRunner.run(conf, new FruitDriver(), args);

    }
}
