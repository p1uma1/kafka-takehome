package org.uor.takehome;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");

        props.put("group.id", "payment-service");

        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(
                Collections.singletonList("payment-requests")
        );

        while (true) {

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {

                System.out.println(
                        "Received payment: " + record.value()
                );

                processPayment(record.value());
            }
        }
    }

    private static void processPayment(String payment) {

        String[] parts = payment.split(",");

        String paymentId = parts[0];
        String orderId = parts[1];
        double amount = Double.parseDouble(parts[2]);

        System.out.println("Payment ID: " + paymentId);
        System.out.println("Order ID: " + orderId);
        System.out.println("Charging: Rs. " + amount);

        // Imagine calling Stripe / bank / payment gateway here

        System.out.println("Payment successful!");
    }
}