package twitter.kafka.storm.demo.process.bolt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.StringTokenizer;


public class FilterHashTagBolt extends BaseBasicBolt {

    private SpoutOutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtags"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String deliminator = " \t\n\r\f,.:;?![]'";
        StringTokenizer tokenizer = new StringTokenizer(tuple.getValue(0).toString(), deliminator);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (token.startsWith("#") && token.trim().length() > 1) {
                collector.emit(new Values(token));
            }
        }

    }
}
