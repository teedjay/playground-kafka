package com.mycompany.kafkastreams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.nio.CharBuffer;
import java.time.LocalDateTime;

/**
 * Produces messages and sends to Kafka topic using transaction for exactly-once-semantics
 *
 * @author thore
 */
public class TestProducer {

    private final KafkaProducer<String, String> producer;

    public TestProducer() {
        producer = KafkaIntializer.createKafkaProducer();
        producer.initTransactions();
    }

    public void send(int number, int length) {
        try {
            producer.beginTransaction();
            String longText = CharBuffer.allocate(length).toString().replace('\0', '@');
            System.out.printf("Sending %d messages of %d bytes each%n", number, longText.getBytes().length);
            String prefix = LocalDateTime.now().toString() + "_";
            for (int i = 0; i < number; i++) producer.send(new ProducerRecord<>(KafkaIntializer.TEST_TOPIC, prefix + i, longText));
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
