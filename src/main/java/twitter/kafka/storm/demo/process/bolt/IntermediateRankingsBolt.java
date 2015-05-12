package twitter.kafka.storm.demo.process.bolt;

import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import twitter.kafka.storm.demo.process.utils.Rankable;
import twitter.kafka.storm.demo.process.utils.RankableObjectWithFields;

/**
 * This bolt ranks incoming objects by their count.
 * <p/>
 * It assumes the input tuples to adhere to the following format: (object, object_count, additionalField1,
 * additionalField2, ..., additionalFieldN).
 * From Storm Starter
 */
public final class IntermediateRankingsBolt extends AbstractRankerBolt {

    private static final long serialVersionUID = -1369800530256637409L;
    private static final Logger LOG = Logger.getLogger(IntermediateRankingsBolt.class);

    public IntermediateRankingsBolt() {
        super();
    }

    public IntermediateRankingsBolt(int topN) {
        super(topN);
    }

    public IntermediateRankingsBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple) {
        Rankable rankable = RankableObjectWithFields.from(tuple);
        super.getRankings().updateWith(rankable);
    }

    @Override
    Logger getLogger() {
        return LOG;
    }
}
