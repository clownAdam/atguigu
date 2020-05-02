package com.atguigu.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author clown
 */
public class HdfsReducer extends TableReducer<NullWritable, Put,NullWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        /*遍历写出*/
        for (Put value : values) {
            context.write(NullWritable.get(),value);
        }
    }
}
