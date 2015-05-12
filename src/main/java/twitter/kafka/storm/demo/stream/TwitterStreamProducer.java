package twitter.kafka.storm.demo.stream;

import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import twitter4j.*;

public class TwitterStreamProducer {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(TwitterStreamProducer.class);

    private final Producer<String, String> producer;
    private Properties producerProperties;

    public TwitterStreamProducer() {
        ProducerConfig config = getProducerConfig();
        producer = new Producer<>(config);
    }

    private ProducerConfig getProducerConfig() {
        producerProperties = new Properties();
        try {
            producerProperties.load(this.getClass().getClassLoader()
                    .getResourceAsStream("kafka-producer.properties"));
        } catch (IOException e) {
            LOGGER.error("Unable to load kafka-producer.properties", e);
        }

        return new ProducerConfig(producerProperties);
    }

    public static void main(String[] args) throws Exception {
        TwitterStreamProducer simpleProducer = new TwitterStreamProducer();
        simpleProducer.twitterStream();
    }

    public void twitterStream() {
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                LOGGER.info("@" + status.getUser().getScreenName() + " - " + status.getText());
                KeyedMessage<String, String> data = new KeyedMessage<>(producerProperties.getProperty("topic")
                        , status.getText());
                producer.send(data);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                LOGGER.info("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                LOGGER.info("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                LOGGER.info("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                LOGGER.info("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                LOGGER.error(ex.getMessage(), ex);
            }
        };

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample("en");
    }

}
