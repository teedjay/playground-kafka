package com.mycompany.kafkastreams;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

/**
 * Consumes messages from the Kafka topic using the streams API
 *
 * @author thore
 */
public class StreamConsumer {
    
    public static void main(final String[] args) throws Exception {
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-group-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        StreamsConfig config = new StreamsConfig(props);
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> items = builder.stream("test-topic");
        items.foreach((k,v) -> System.out.printf("%s=%s\n",k, v.substring(0, 10)));
 
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
        
    }
    
}
