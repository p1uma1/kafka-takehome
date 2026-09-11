package org.uor.takehome;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

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
                producer.send(record, (metadata, exception) -> {

                    if (exception != null) {

                        System.err.println(
                                "\nOrder failed: " + exception.getMessage()
                        );

                    } else {

                        System.out.println(
                                "Order sent: "
                                + orderToSend.getOrderId() + " | "
                                + orderToSend.getProduct() + " | Rs. "
                                + orderToSend.getPrice()
                        );
                    }
                });

            } catch (KafkaException e) {
                // We can't recover from these exceptions, so our only option is to close the producer and exit.

            }
            //     Example usage
            //     try {
            //         producer.beginTransaction();
            //         for (int i = 0; i < 100; i++) {
            //             producer.s end(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
            //         }
            //         producer.commitTransaction();
            //     } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            //         // We can't recover from these exceptions, so our only option is to close the producer and exit.
            //         producer.close();
            //     } catch (KafkaException e) {
            //         // For all other exceptions, just abort the transaction and try again.
            //         producer.abortTransaction();
            //     }
            //     producer.close();

            orderId++;
        }

    }
}
