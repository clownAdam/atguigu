package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Properties;

/**
 * @author clown
 */
public class CustomerConsumer {
    /**
     * 配置信息
     */
    public static Properties props = new Properties();
    static {
        //kafka集群
        props.put("bootstrap.servers", "bd-101:9092");
        //消费者组ID
        props.put("group.id", "test-consumer-group");
//        props.put("group.id", "test-consumr");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //设置自动提交offset
        props.put("enable.auto.commit", "true");
        //设置自动提交延时
        props.put("auto.commit.interval.ms", "1000");
        /*kv的反序列化*/
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public static void main(String[] args) {
        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //指定topic
//        consumer.subscribe(Arrays.asList("second"));
//        consumer.subscribe(Collections.singletonList("second"));
        consumer.assign(Collections.singletonList(new TopicPartition("second",0)));
        consumer.seek(new TopicPartition("second",0),2);
        while (true) {
            //获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record.topic() + "---" + record.partition() + "---" + record.value());
            }
        }
    }
}
