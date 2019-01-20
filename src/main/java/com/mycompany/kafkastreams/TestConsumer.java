package com.mycompany.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;

import static java.time.Duration.ofSeconds;

/**
 * Consumes messages from the Kafka topic using the standard consumer api
 *
 * @author thore
 */
public class TestConsumer {

    private final KafkaConsumer<String, String> consumer;

    public TestConsumer() {
        consumer = KafkaIntializer.createKafkaConsumer();
    }

    public void receive(long durationSeconds) {
        boolean firstTimeReset = false; // beware reset to start of time
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(ofSeconds(durationSeconds));
            if (firstTimeReset) {
                firstTimeReset = false;
                consumer.seekToBeginning(Collections.singleton(new TopicPartition(KafkaIntializer.TEST_TOPIC, 0)));
            }
            System.out.println("Polling ...");
            records.records(new TopicPartition(KafkaIntializer.TEST_TOPIC, 0)).forEach(
                    r -> { System.out.printf("offset = %d, key = %s, value = %s\n", r.offset(), r.key(), r.value().substring(0, 10)); }
                    );
        }
    }

    public static void main(String[] args) {
        TestConsumer tp = new TestConsumer();
        tp.receive(3);
    }

}
