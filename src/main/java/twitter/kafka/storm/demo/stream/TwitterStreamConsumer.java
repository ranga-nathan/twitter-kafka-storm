package twitter.kafka.storm.demo.stream;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import twitter.kafka.storm.demo.main.Application;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZkUtils;

public class TwitterStreamConsumer {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(TwitterStreamConsumer.class);

    private final ConsumerConnector consumer;
    private Properties consumerProperties;
    private ExecutorService executor;

    public TwitterStreamConsumer() {

        consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(getConsumerConfig());
        resetOffset();
    }

    private void resetOffset() {
        ZkUtils.maybeDeletePath(consumerProperties.getProperty("zookeeper.connect"), "/consumers/" + consumerProperties.getProperty("group.id"));
        //ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + partition, offset.toString)
    }

    private ConsumerConfig getConsumerConfig() {
        consumerProperties = new Properties();
        try {
            consumerProperties.load(this.getClass().getClassLoader()
                    .getResourceAsStream("kafka-consumer.properties"));
        } catch (IOException e) {
            LOGGER.error("Unable to load kafka-consumer.properties", e);
        }

        return new ConsumerConfig(consumerProperties);
    }

    public void consumeWithSingleThread() {
        Map<String, Integer> topicMap = new HashMap<>();
        int numOfThreads = 1;
        topicMap.put(consumerProperties.getProperty("topic"), new Integer(numOfThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap =
                consumer.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap
                .get(consumerProperties.getProperty("topic"));

        for (final KafkaStream<byte[], byte[]> stream : streamList) {
            ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();
            while (consumerIterator.hasNext())
                LOGGER.info(" Message from Single Topic :: "
                        + new String(consumerIterator.next().message()));
        }
        if (consumer != null)
            consumer.shutdown();
    }

    public void consume() {
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(consumerProperties.getProperty("topic"), new Integer(consumerProperties.getProperty("consumer.threads")));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap =
                consumer.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap
                .get(consumerProperties.getProperty("topic"));

        // Launching the thread pool
        executor = Executors.newFixedThreadPool(new Integer(consumerProperties.getProperty("consumer.threads")));
        // Creating an object messages consumption
        int threadCount = 0;
        for (final KafkaStream<byte[], byte[]> stream : streamList) {
            new Thread(new TweetConsumer(stream, threadCount)).start();
            Application.cousumerCountMap.put(String.valueOf(threadCount), new AtomicInteger(0));
            threadCount++;
        }
    }

    public void shutdown() {
        if (consumer != null)
            consumer.shutdown();
        if (executor != null)
            executor.shutdown();

        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                LOGGER.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted during shutdown, exiting uncleanly", e);
        }
    }

    public static void main(String[] args) {
        TwitterStreamConsumer streamConsumer = new TwitterStreamConsumer();
        streamConsumer.consume();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
        streamConsumer.shutdown();

    }

}
