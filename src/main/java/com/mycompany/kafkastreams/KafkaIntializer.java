package com.mycompany.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class KafkaIntializer {

    public static final String TEST_TOPIC = "test-topic";

    public static KafkaConsumer<String, String> createKafkaConsumer() {
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
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(singleton(TEST_TOPIC));
        return consumer;
    }

    public static KafkaProducer<String, String> createKafkaProducer() {
        // https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "er-ts-appqa04.os.eon.no:9092");
        props.put(TRANSACTIONAL_ID_CONFIG, "my-trans-id");
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

}
