package com.haoge.interceptor;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;

public class MyProducer1 {

    public static void main(String[] args) throws InterruptedException {
        ArrayList<String> interceptors = new ArrayList<String>();
        interceptors.add("com.haoge.interceptor.TimeInterceptor");
        interceptors.add("com.haoge.interceptor.CounterInterceptor");
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

        //自定义拦截器
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG , interceptors );

        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //添加自己的分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.haoge.producer.MyPatitioner");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        //循环写入
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first","haoge" + i));
        }

        producer.close();
    }
}
