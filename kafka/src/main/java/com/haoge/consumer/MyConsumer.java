package com.haoge.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class MyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //连接kafka集群
        props.put("bootstrap.servers", "hadoop102:9092");

        //设置组id
        props.put("group.id", "test");
        //自动提交，默认的
        props.put("enable.auto.commit", "false");
        //自动提交间隔时间
        props.put("auto.commit.interval.ms", "1000");
        //反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //设置从头开始读取,需要满足offset被删除，以及offet为0两个条件，选择为0，修改组id即可
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //订阅主题
        consumer.subscribe(Arrays.asList("first"));

        //长轮询拉取数据
        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("Topic:" + consumerRecord.topic()
                        + "Partition:" + consumerRecord.partition()
                        +"Offset:" + consumerRecord.offset()
                        +"Key:" + consumerRecord.key()
                        +"Value:" + consumerRecord.value());
            }
            //同步提交：相对于自动提交可靠一些，但是阻塞线程，对吞吐量影响大，失败可重试
            //consumer.commitSync();

            //更多情况下选择异步提交，失败不可重试，
            consumer.commitAsync(new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if( exception==null ){
                        Set<Map.Entry<TopicPartition, OffsetAndMetadata>> entries = offsets.entrySet();
                        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : entries) {
                            System.out.println(entry.getKey() + "==" + entry.getValue());
                        }
                    }
                }
            });
        }

    }
}
