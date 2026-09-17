package org.uor.takehome;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class Consumer {

    public static void main(String[] args) {

        // Kafka Streams configuration
        Properties props = new Properties();

        props.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "price-average-app"
        );

        props.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        // Schema Registry configuration
        Map<String, String> serdeConfig = Map.of(
                "schema.registry.url",
                "http://localhost:8081"
        );

        // Order Avro Serde
        SpecificAvroSerde<Order> orderSerde
                = new SpecificAvroSerde<>();

        orderSerde.configure(serdeConfig, false);

        // PriceAggregate Avro Serde
        SpecificAvroSerde<PriceAggregate> aggregateSerde
                = new SpecificAvroSerde<>();

        aggregateSerde.configure(serdeConfig, false);

        // Build topology
        StreamsBuilder builder = new StreamsBuilder();

        // Read Orders from Kafka
        KStream<String, Order> orders
                = builder.stream(
                        "orders",
                        Consumed.with(
                                Serdes.String(),
                                orderSerde
                        )
                );
        KStream<String, Order> savedOrders
                = orders.filter((key, order) -> {
                System.out.println("order recieved");
                    return saveOrderWithRetry(order);

                });

        // Aggregate running sum + count
        KTable<String, PriceAggregate> aggregate
                = savedOrders
                        .groupBy(
                                (key, order) -> "all",
                                Grouped.with(
                                        Serdes.String(),
                                        orderSerde
                                )
                        )
                        .aggregate(
                                () -> PriceAggregate.newBuilder()
                                        .setSum(0.0)
                                        .setCount(0L)
                                        .build(),
                                (key, order, current) -> {

                                    System.out.println("New Order : " + order.getProduct());
                                    double price
                                    = ((Number) order.getPrice())
                                            .doubleValue();

                                    return PriceAggregate.newBuilder()
                                            .setSum(
                                                    current.getSum()
                                                    + price
                                            )
                                            .setCount(
                                                    current.getCount()
                                                    + 1
                                            )
                                            .build();
                                },
                                Materialized.with(
                                        Serdes.String(),
                                        aggregateSerde
                                )
                        );

        // Calculate running average
        KTable<String, Double> averages
                = aggregate.mapValues(stats -> {

                    if (stats.getCount() == 0) {
                        return 0.0;
                    }

                    return stats.getSum()
                            / stats.getCount();
                });

        // Print updated average
        averages
                .toStream()
                .peek(
                        (key, average)
                        -> System.out.println(
                                "Running average: "
                                + average
                        )
                );

        // Start Kafka Streams
        KafkaStreams streams
                = new KafkaStreams(
                        builder.build(),
                        props
                );

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(streams::close)
                );

        streams.start();
    }

    private static boolean saveOrderWithRetry(Order order) {

        int maxAttempts = 3;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {

            try {

                Database.saveOrder(order);

                System.out.println(
                        "Saved to database: "
                        + order.getOrderId()
                );

                return true;

            } catch (Exception e) {

                System.err.println(
                        "Attempt " + attempt
                        + " failed: "
                        + e.getMessage()
                );

                if (attempt == maxAttempts) {

                    System.err.println(
                            "All attempts failed for order: "
                            + order.getOrderId()
                    );

                    return false;
                }

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }

        return false;
    }
}
