package org.uor.takehome;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        Properties props = new Properties();

        //config kafka url
        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(
                "schema.registry.url",
                "http://localhost:8081"
        );
        props.put("enable.idempotence", true);

        KafkaProducer<String, Order> producer
                = new KafkaProducer<>(props);

        int orderId = 1;

        while (true) {

            System.out.println("\n===== ORDER SYSTEM =====");
            System.out.println("1. Soap       - Rs. 150");
            System.out.println("2. Shampoo    - Rs. 450");
            System.out.println("3. Toothpaste - Rs. 250");
            System.out.println("0. Exit");

            System.out.print("Enter your choice: ");

            int choice = scanner.nextInt();

            Order order = null;

            switch (choice) {

                case 1:
                    order = new Order(
                            String.format("%04d", orderId),
                            "Soap",
                            150.0f
                    );
                    break;

                case 2:
                    order = new Order(
                            String.format("%04d", orderId),
                            "Shampoo",
                            450.0f
                    );
                    break;

                case 3:
                    order = new Order(
                            String.format("%04d", orderId),
                            "Toothpaste",
                            250.0f
                    );
                    break;

                case 0:
                    System.out.println("Exiting...");
                    producer.close();
                    scanner.close();
                    return;

                default:
                    System.out.println("Invalid choice.");
                    continue;
            }
            if (order == null) {
                continue;
            }

            ProducerRecord<String, Order> record
                    = new ProducerRecord<>(
                            "orders",
                            order.getOrderId().toString(),
                            order
                    );
            final Order orderToSend = order;

            try {

                var metadata = producer.send(record).get();

                System.out.println(
                        "Order REALLY sent: "
                        + order.getOrderId()
                        + " | " + order.getProduct()
                        + " | Rs. " + order.getPrice()
                );

                System.out.println(
                        "Topic: " + metadata.topic()
                        + " Partition: " + metadata.partition()
                        + " Offset: " + metadata.offset()
                );

            } catch (Exception e) {

                System.err.println("PRODUCER ERROR:");
                e.printStackTrace();
            }

            orderId++;
        }

    }

}
