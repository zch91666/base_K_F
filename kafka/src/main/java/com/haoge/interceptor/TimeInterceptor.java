package com.haoge.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor {
    public ProducerRecord onSend(ProducerRecord record) {
        return new ProducerRecord(record.topic(),record.partition(),record.key(),System.currentTimeMillis()+":"+record.value());
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
