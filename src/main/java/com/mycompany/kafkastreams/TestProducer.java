package com.mycompany.kafkastreams;

import java.nio.CharBuffer;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Produces messages and sends to Kafka topic
 *
 * @author thore
 */
public class TestProducer {

    private final Properties props;

    public TestProducer() {
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);   
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public void send(int number) {
        String longText = CharBuffer.allocate(16384).toString().replace('\0', '@');
        System.out.println("Number of bytes pr msg : " + longText.getBytes().length);
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for(int i = 0; i < number; i++)
                producer.send(new ProducerRecord<String, String>("test-topic", Integer.toString(i), longText));
        }
    }
    
    public static void main(String[] args) {
        int numbersToSend = 100;
        long nanosStart = System.nanoTime();
        TestProducer tp = new TestProducer();
        long nanosProducer = System.nanoTime();
        tp.send(numbersToSend);
        long nanosFinished = System.nanoTime();
        long startProducer = (nanosProducer - nanosStart) / 1000000;
        long sendMessages = (nanosFinished - nanosProducer) / 1000000;
        System.out.printf("Startup %dms, sending %d msgs in %dms\n", startProducer, numbersToSend, sendMessages);
    }
    
    

}
