package twitter.kafka.storm.demo.process.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter.kafka.storm.demo.process.bolt.*;
import twitter.kafka.storm.demo.process.spout.TwitterKafkaSpout;

public class RollingCountTopologyRunner {

    public static final Logger LOGGER = LoggerFactory.getLogger(RollingCountTopologyRunner.class);

    private static final int DEFAULT_RUNTIME_IN_SECONDS = 180;
    private static final int TOP_N = 5;
    private LocalCluster cluster;

    private final static String LOCAL_NAME = "twitter-kafka-storm-local";
    private final static String CLUSTER_NAME = "twitter-kafka-storm-cluster";

    public RollingCountTopologyRunner() {

    }

    public StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweets", new TwitterKafkaSpout(TwitterKafkaSpout.getKafkaSpoutConfig()), 3).setNumTasks(3);
        builder.setBolt("hashtags", new FilterHashTagBolt(), 5).shuffleGrouping("tweets");
        builder.setBolt("rolling-count", new RollingCountBolt(DEFAULT_RUNTIME_IN_SECONDS, 60), 4).shuffleGrouping("hashtags");
        builder.setBolt("intermediate-count", new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping("rolling-count", new Fields("obj"));
        builder.setBolt("total-rankings", new TotalRankingsBolt(TOP_N)).globalGrouping("intermediate-count");
        builder.setBolt("persist", new PersistBolt()).globalGrouping("total-rankings");
        return builder.createTopology();
    }

    public void runLocal() {
        cluster = new LocalCluster();
        cluster.submitTopology(LOCAL_NAME, buildConfiguration(), buildTopology());
        LOGGER.info("Local cluster started - "+LOCAL_NAME);
    }

    public void stopLocal() {
        LOGGER.info("Trying to stop Local cluster - "+LOCAL_NAME);
        cluster.killTopology(LOCAL_NAME);
        cluster.shutdown();
        LOGGER.info("Successfully Local cluster - " + LOCAL_NAME);
    }

    public void runCluster() throws AlreadyAliveException, InvalidTopologyException {
        StormSubmitter.submitTopology(CLUSTER_NAME, buildConfiguration(), buildTopology());
    }

    private Config buildConfiguration() {
        Config conf = new Config();
        conf.setNumWorkers(3);
        return conf;
    }
}
