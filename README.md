# twitter-kafka-storm
Sample Twitter stream with Kafka and Storm processing in Java.

1. Get Twitter app credentials and Add your twitter credentials to twitter4j.properties
2. Start Kafka multi broker cluster: https://kafka.apache.org/08/quickstart.html 
3. create topic bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic test-parts
4. ./gradlew bootRun 
