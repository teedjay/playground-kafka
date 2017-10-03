package com.mycompany.kafkastreams;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;

/**
 * Connects to the Kafka broker and list available topics and cluster topology
 *
 * @author thore
 */
public class AdminClientTest {
    
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(config);
        
        ListTopicsResult topics = admin.listTopics();
        topics.names().get().forEach(p -> System.out.printf("topic = %s\n", p));
        
        DescribeClusterResult cluster = admin.describeCluster();
        cluster.nodes().get().forEach(c -> System.out.printf("node = %s\n", c.toString()));
    }
    
}
