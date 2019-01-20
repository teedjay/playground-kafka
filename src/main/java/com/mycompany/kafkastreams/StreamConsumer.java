package com.mycompany.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.time.LocalDateTime;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;

/**
 * Consumes messages from the Kafka topic using the streams API
 *
 * @author thore
 */
public class StreamConsumer {

    private static final String THE_TOPIC = "test-topic";

    public static void main(final String[] args) throws Exception {
        
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "er-ts-appqa04.os.eon.no:9092");
        props.put(APPLICATION_ID_CONFIG, "my-stream-id");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(COMMIT_INTERVAL_MS_CONFIG, 1000);

        // props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        // props.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        // props.put(ENABLE_AUTO_COMMIT_CONFIG, "true");

        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> items = builder.stream(THE_TOPIC);
        items.foreach((k,v) -> System.out.printf("[%s] = %s\n",k, v.substring(0, 10)));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            throwable.printStackTrace();
        });

        streams.start();

        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        while (true) {
            Thread.sleep(3000L);
            System.out.println("Streaming ... @ " + LocalDateTime.now());
        }

        // streams.close();

    }
    
}
