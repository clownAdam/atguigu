package com.atguigu.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @author clown
 */
public class KafkaStream {
    public static void main(String[] args) {
        /*创建拓扑对象*/
        TopologyBuilder builder  = new TopologyBuilder();
        /*创建配置对象*/
        Properties properties = new Properties();
        properties.put("bootstrap.servers","bd-101:9092");
        properties.put("application.id","KafkaStream");
        /*构建拓扑结构*/
        builder.addSource("SOURCE","first")
        .addProcessor("PROCESSOR", new LogProcessorSupplier(), "SOURCE")
        .addSink("SINK","second", "PROCESSOR")
        ;
        KafkaStreams kafkaStreams = new KafkaStreams(builder, properties);

        kafkaStreams.start();
    }
}
class LogProcessorSupplier implements ProcessorSupplier{

    @Override
    public Processor get() {
        return new LogProcessor();
    }
}
class LogProcessor implements Processor<byte[],byte[]>{
    ProcessorContext context = null;
    @Override
    public void init(ProcessorContext processorContext) {
        context = processorContext;
    }

    @Override
    public void process(byte[] bytes, byte[] bytes2) {
        String line = new String(bytes2);
        line = line.replaceAll(">>>","");
        bytes2 = line.getBytes();
        context.forward(bytes,bytes2);
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
