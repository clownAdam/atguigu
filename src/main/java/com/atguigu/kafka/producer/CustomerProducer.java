package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author clown
 */
public class CustomerProducer {
    /**
     * 配置信息
     */
    public static Properties props = new Properties();

    static {
        //kafka集群
        props.put("bootstrap.servers", "bd-101:9092");
        //应答级别
        props.put("acks", "all");
        //重试次数
        props.put("retries", 0);
        //批量大小
        props.put("batch.size", 16384);
        //提交延时
        props.put("linger.ms", 1);
        //缓存
        props.put("buffer.memory", 33554432);
        //kv的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //关联自定义分区类
//        props.put("partitioner.class","com.atguigu.kafka.producer.partition.CustomerPartitioner");
        //关联拦截器
        ArrayList<String> list = new ArrayList<>();
        list.add("com.atguigu.kafka.interceptor.TimeIntercepor");
        list.add("com.atguigu.kafka.interceptor.CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,list);
    }

    public static void main(String[] args) {

        /*创建生产者对象*/
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        /*
         * 循环发送数据,case is 不带回调函数
         * <pre>
         * {@code
         * for (int i = 0; i < 10; i++)
         *   producer.send(new ProducerRecord<>("first",String.valueOf(i+"--->")));
         * }</pre>
         * */
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("second", String.valueOf(i)), (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("发送失败");
                } else {
                    System.out.println(metadata.partition() + "---" + metadata.offset());
                }
            });
        }
        /*关闭资源*/
        producer.close();
    }
}
