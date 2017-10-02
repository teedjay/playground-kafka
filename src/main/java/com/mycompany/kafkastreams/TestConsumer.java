package com.mycompany.kafkastreams;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Consumes messages from the Kafka topic using the standard consumer api
 *
 * @author thore
 */
public class TestConsumer {

    private final Properties props;

    public TestConsumer() {
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group-consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public void receive(long timeoutMilliSeconds) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeoutMilliSeconds);
            System.out.println("Number of records read " + records.count());
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value().substring(0, 10));
            }
        }
    }

    public static void main(String[] args) {
        TestConsumer tp = new TestConsumer();
        tp.receive(1000L);
    }

}
