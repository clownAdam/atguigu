package com.atguigu.kafka.low;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 根据指定的topic,partition,offset来获取数据
 *
 * @author clown
 */
public class LowerConsumer {

    public static void main(String[] args) {
        /*定义相关参数*/
        //kafka集群
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("bd-101");
        brokers.add("bd-102");
        brokers.add("bd-103");
        brokers.add("bd-104");
        brokers.add("bd-105");
        //端口号
        int port = 9092;
        //topic：主题
        String topic = "second";
        //partition:分区
        int partition = 0;
        //offset
        long offset = 2;
        LowerConsumer lowerConsumer = new LowerConsumer();
        lowerConsumer.getData(brokers,port, topic, partition, offset);
    }

    /**
     * 找分区leader
     */
    private BrokerEndPoint findLeader(List<String> brokers, int port, String topic, int partition) {
        for (String broker : brokers) {
            /*获取分区leader的消费者对象*/
            SimpleConsumer getLeader = new SimpleConsumer(broker, port, 1000, 1024 * 4, "getLeader");
            /*创建一个主题元数据信息请求*/
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            /*获取主题元数据返回值*/
            TopicMetadataResponse metadataResponse = getLeader.send(topicMetadataRequest);
            /*解析元数据返回值*/
            List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
            /*遍历主题元数据*/
            for (TopicMetadata topicMetadata : topicsMetadata) {
                /*获取多个分区的元数据信息*/
                List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
                for (PartitionMetadata partitionMetadata : partitionsMetadata) {
                    if(partition == partitionMetadata.partitionId()){
                        return partitionMetadata.leader();
                    }
                }
            }
        }
        return null;
    }
    /**
     * 获取数据
     */
    private void getData(List<String> brokers, int port, String topic, int partition,long offset) {
        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
        if (leader == null){
            return;
        }
        String leaderHost = leader.host();
        /*获取数据的消费者对象*/
        SimpleConsumer getData = new SimpleConsumer(leaderHost, port, 1000, 1024 * 4, "getData");
        /*创建获取数据的对象*/
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 100000).build();
        /*获取数据返回值*/
        FetchResponse fetchResponse = getData.fetch(fetchRequest);
        /*解析返回值*/
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            long offset1 = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(offset1+"----"+new String(bytes));
        }
    }

}
