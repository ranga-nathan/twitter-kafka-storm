package twitter.kafka.storm.demo.stream;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import twitter.kafka.storm.demo.main.Application;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class TweetConsumer implements Runnable {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(TweetConsumer.class);

    private KafkaStream kafkaStream;
    private Integer threadNumber;

    public TweetConsumer(KafkaStream kafkaStream, Integer threadNumber) {
        this.threadNumber = threadNumber;
        this.kafkaStream = kafkaStream;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
        System.out.println("Created iterator " + it.toString() + " thread number " + threadNumber);
        System.out.println(it.hasNext() + " - "+ threadNumber);

        while (it.hasNext()) {
            TweetJsonDecoder decoder = new TweetJsonDecoder(null);
            String map = (String) decoder.fromBytes(it.next().message());
            try {
                Status s = TwitterObjectFactory.createStatus(map);
                AtomicInteger c = Application.cousumerCountMap.get(String.valueOf(threadNumber));
                c = (c == null)? new AtomicInteger(0): c;
                Application.cousumerCountMap.put(String.valueOf(threadNumber), new AtomicInteger(c.incrementAndGet()));
                LOGGER.info("Thread " + threadNumber + " " + s.getSource());
            } catch (TwitterException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        LOGGER.info("Shutting down Thread: " + threadNumber);
    }
}
