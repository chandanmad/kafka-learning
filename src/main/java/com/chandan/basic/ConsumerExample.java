package com.chandan.basic;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class ConsumerExample {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("group.id", "test-grou");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        properties.put("auto.offset.reset", "earliest");
        Consumer<Long, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("test"));

        ConsumerRecords<Long, String> records = consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
        records.forEach(r -> {
            System.out.println(r.key());
            System.out.println(r.value());
            System.out.println(r.offset());
            r.leaderEpoch().ifPresent(i -> System.out.println("LE " + i));
        });
        consumer.commitSync();
        consumer.close();
    }
}
