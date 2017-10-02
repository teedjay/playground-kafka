# playground-kafka
Small tests for Kafka 0.11 consumer api and streaming api, 
showing how to send, pull and stream messages using Java 8.

## Prerequisite 
In order to run these small examples you need a running Kafka broker on 
localhost port 9092 with a topic called ```test-topic```.

## Quickstart Kafka broker
Start a Kafka broker locally by using [Blacktop Kafka Docker image](https://hub.docker.com/r/blacktop/kafka/).

```
docker run -d \
           --name kafka \
           -p 9092:9092 \
           -e KAFKA_ADVERTISED_HOST_NAME=localhost \
           -e KAFKA_CREATE_TOPICS="test-topic:1:1" \
           blacktop/kafka
```

More info about Kafka can be found in the [Kafka Documentation](https://kafka.apache.org/documentation/)

When the Docker container has started you can start and stop as needed. 

```
docker stop kafka
docker start kafka

docker ps -a (lists all docker containers, even those not running)
```

## Running the demos
There are two consumer examples, they connect to the broker using different group.id / application.id
so they will both receive all messages and they can be run in parallel.

- [ ] TestAdminClient, connects to broker and list topics and cluster info
- [ ] TestProducer, connects to broker and send x messages of y size
- [ ] TestConsumer, connects to broker and pulls messages
- [ ] StreamConsumer, connects to broker and streams messages


 