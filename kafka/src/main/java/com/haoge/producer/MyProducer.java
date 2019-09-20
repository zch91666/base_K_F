package com.haoge.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class MyProducer {
    public static void main(String[] args) throws InterruptedException {
        //1.配置文件
        Properties props = new Properties();
        //连接
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //指定ack
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试连接次数
        props.put("retries", 3);
        //批次数据大小
        props.put("batch.size", 16384);
        //连接时间
        props.put("linger.ms", 1);

        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //添加自己的分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.haoge.producer.MyPatitioner");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        //循环写入
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first","haoge" + i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if( metadata!=null ){
                        System.out.println("offset:"+metadata.offset());
                        System.out.println("partition:"+metadata.partition());
                        System.out.println("topic:"+metadata.topic());
                    }
                }
            });
        }

       producer.close();
    }
}
