import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {

    public static void main(String[] args) {

        Properties props = new Properties();

        //config kafka url
        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        String paymentRequest =
                "PAY-1001,ORDER-42,2500.00";

        ProducerRecord<String, String> record =
                new ProducerRecord<>(
                        "payment-requests",
                        "ORDER-42",
                        paymentRequest
                );

        producer.send(record);

        producer.close();
    }
}