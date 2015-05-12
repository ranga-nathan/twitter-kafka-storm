package twitter.kafka.storm.demo.process.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;


import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class TwitterKafkaSpout extends KafkaSpout {

    public static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaSpout.class);

    public static SpoutConfig getKafkaSpoutConfig() {

        Properties consumerProperties = new Properties();
        try {
            consumerProperties.load(TwitterKafkaSpout.class.getClassLoader()
                    .getResourceAsStream("kafka-consumer.properties"));
        } catch (IOException e) {
            LOGGER.error("Unable to load kafka-consumer.properties", e);
        }

        GlobalPartitionInformation hostsAndPartitions = new GlobalPartitionInformation();
        hostsAndPartitions.addPartition(0, new Broker("localhost", 9093));
        //hostsAndPartitions.addPartition(1, new Broker("localhost", 9093));
        //hostsAndPartitions.addPartition(2, new Broker("localhost", 9094));
        BrokerHosts brokerHosts = new StaticHosts(hostsAndPartitions);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, consumerProperties.getProperty("topic"), "/data/kafka", UUID.randomUUID().toString());
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        return kafkaConfig;

    }

    public TwitterKafkaSpout(SpoutConfig kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}
