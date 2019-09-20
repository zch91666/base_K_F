package com.haoge.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor {

    private int Success;
    private int Error;

    public ProducerRecord onSend(ProducerRecord record) {
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if( metadata != null ){
            Success++;
        }else{
            Error++;
        }
    }

    public void close() {
        System.out.println("Success:" + Success + "条,Error:" + Error + "条");
    }

    public void configure(Map<String, ?> configs) {

    }
}
