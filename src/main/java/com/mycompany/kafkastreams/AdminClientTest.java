package com.mycompany.kafkastreams;

import org.apache.kafka.clients.admin.*;

import java.util.Properties;

/**
 * Connects to the Kafka broker and list available topics and cluster topology
 *
 * @author thore
 */
public class AdminClientTest {
    
    public static void main(String[] args) throws Exception {

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "er-ts-appqa04.os.eon.no:9092");
        AdminClient admin = AdminClient.create(config);
        
        ListTopicsResult topics = admin.listTopics();
        topics.names().get().forEach(p -> System.out.printf("topics = %s%n", p));
        
        DescribeClusterResult cluster = admin.describeCluster();
        cluster.nodes().get().forEach(c -> System.out.printf("nodes = %s%n", c.toString()));

        ListConsumerGroupsResult listConsumerGroupsResult = admin.listConsumerGroups();
        listConsumerGroupsResult.all().get().forEach(g -> dumpOffsets(admin, g.groupId()));

    }

    private static void dumpOffsets(AdminClient admin, String groupId) {
        try {
            ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = admin.listConsumerGroupOffsets(groupId);
            listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get().forEach((t, o) -> System.out.printf("topic = %s [group = %s] (offset %d)%n", t.topic(), groupId, o.offset()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
