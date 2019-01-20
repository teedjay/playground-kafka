package com.mycompany.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

/**
 * Consumes messages from the Kafka topic using the streams API
 *
 * @author thore
 */
public class StreamConsumer {
    
    public static void main(final String[] args) throws Exception {
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app-id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "er-ts-appqa04.os.eon.no:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /*
        props.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        */

        //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = builder.build();

        KStream<String, String> items = builder.stream(KafkaIntializer.TEST_TOPIC);
        items.foreach((k,v) -> System.out.printf("%s=%s\n",k, v.substring(0, 10)));

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            throwable.printStackTrace();
        });

        streams.start();

        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        Thread.sleep(50000L);

        streams.close();

    }
    
}
