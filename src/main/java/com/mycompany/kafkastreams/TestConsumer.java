package com.mycompany.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * Consumes messages from the Kafka topic using the standard consumer api
 *
 * @author thore
 */
public class TestConsumer {

    private static final String THE_TOPIC = "test-topic";
    private final KafkaConsumer<String, String> consumer;

    public TestConsumer() {
        // https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "er-ts-appqa04.os.eon.no:9092");
        props.put(GROUP_ID_CONFIG, "my-consumer-group");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest"); // "earliest" (earliest unread) or "latest" (the most current)
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(singleton(THE_TOPIC));
    }

    public void receive(long durationSeconds) {
        boolean firstTimeReset = false; // beware reset to start of time
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(ofSeconds(durationSeconds));
            if (firstTimeReset) {
                firstTimeReset = false;
                consumer.seekToBeginning(Collections.singleton(new TopicPartition(THE_TOPIC, 0)));
            }
            System.out.println("Polling ... @ " + LocalDateTime.now());
            records.records(new TopicPartition(THE_TOPIC, 0)).forEach(
                    r -> { System.out.printf("offset = %d, key = %s, value = %s\n", r.offset(), r.key(), r.value().substring(0, 10)); }
                    );
        }
    }

    public static void main(String[] args) {
        TestConsumer tp = new TestConsumer();
        tp.receive(3);
    }

}
