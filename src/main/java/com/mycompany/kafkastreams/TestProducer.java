package com.mycompany.kafkastreams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.nio.CharBuffer;
import java.time.LocalDateTime;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Produces messages and sends to Kafka topic using transaction for exactly-once-semantics
 *
 * @author thore
 */
public class TestProducer {

    private static final String THE_TOPIC = "test-topic";
    private final KafkaProducer<String, String> producer;

    public TestProducer() {
        // https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "er-ts-appqa04.os.eon.no:9092");
        props.put(TRANSACTIONAL_ID_CONFIG, "my-trans-id");
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        producer.initTransactions();
    }

    public void send(int number, int length) {
        try {
            producer.beginTransaction();
            String longText = CharBuffer.allocate(length).toString().replace('\0', '@');
            System.out.printf("Sending %d messages of %d bytes each%n", number, longText.getBytes().length);
            String prefix = LocalDateTime.now().toString() + "_";
            for (int i = 0; i < number; i++) producer.send(new ProducerRecord<>(THE_TOPIC, prefix + i, longText));
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
    }
    
    public static void main(String[] args) {
        long nanosStart = System.nanoTime();
        TestProducer tp = new TestProducer();
        long nanosProducer = System.nanoTime();
        int messagesToSend = 10;
        int messagesSize = 1024;
        tp.send(messagesToSend, messagesSize);
        long nanosFinished = System.nanoTime();
        long startProducer = (nanosProducer - nanosStart) / 1000000;
        long sendMessages = (nanosFinished - nanosProducer) / 1000000;
        System.out.printf("Startup %dms, sending %d msgs in %dms (total %d bytes)%n", startProducer, messagesToSend, sendMessages, 0L + messagesSize * messagesToSend);
    }
    
}
