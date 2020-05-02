package com.atguigu.kafka.producer.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author clown
 * 自定义分区的生产者
 */
public class CustomerPartitioner implements Partitioner {
    /**
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return int
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        return 0;
    }

    /**
     *
     */
    @Override
    public void close() {

    }

    /**
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {
    }
}
