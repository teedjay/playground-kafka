package com.mycompany.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Consumes messages from the Kafka topic using the streams API
 *
 * @author thore
 */
public class StreamConsumer {
    
    public static void main(final String[] args) throws Exception {
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app-stream-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "er-ts-appqa04.os.eon.no:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            throwable.printStackTrace();
        });

        streams.start();

        KStream<String, String> items = builder.stream(KafkaIntializer.TEST_TOPIC);
        items.foreach((k,v) -> System.out.printf("%s=%s\n",k, v.substring(0, 10)));

        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        Thread.sleep(50000L);

        streams.close();

    }
    
}
