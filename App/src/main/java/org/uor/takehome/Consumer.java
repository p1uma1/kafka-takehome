package org.uor.takehome;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class Consumer {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");

        props.put("group.id", "payment-service");

        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer",
                "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        props.put("auto.offset.reset", "earliest");

        props.put(
                "schema.registry.url",
                "http://localhost:8081"
        );
        props.put(
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
                true
        );

        KafkaConsumer<String, Order> consumer
                = new KafkaConsumer<>(props);

        consumer.subscribe(
                Collections.singletonList("orders")
        );

        while (true) {

            ConsumerRecords<String, Order> records
                    = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, Order> record : records) {

                System.out.println(
                        "Received order: " + record.value().getProduct()
                );
            }
        }
    }
}
