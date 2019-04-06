package com.chandan.basic;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerExample {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("acks", "all");
        producerProperties.put("key.serializer", StringSerializer.class);
        producerProperties.put("value.serializer", StringSerializer.class);
        Producer<Long, String> producer = new KafkaProducer<>(producerProperties);
        System.out.println("Offset " + producer.send(new ProducerRecord<>("test", "hello1")).get().offset());
        producer.close();
    }
}
