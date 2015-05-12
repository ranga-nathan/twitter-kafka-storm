package twitter.kafka.storm.demo.process.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter.kafka.storm.demo.main.Application;
import twitter.kafka.storm.demo.process.utils.Rankings;

public class PersistBolt extends BaseBasicBolt {

    public static final Logger LOGGER = LoggerFactory.getLogger(PersistBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer fieldsDeclarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Rankings rankings = (Rankings) tuple.getValue(0);
        Application.rankings = rankings;
        LOGGER.info(rankings.getRankings().toString());
    }

}