package twitter.kafka.storm.demo.main;

import twitter.kafka.storm.demo.process.topology.RollingCountTopologyRunner;
import twitter.kafka.storm.demo.process.utils.Rankable;
import twitter.kafka.storm.demo.process.utils.Rankings;
import twitter.kafka.storm.demo.stream.TwitterStreamConsumer;
import twitter.kafka.storm.demo.stream.TwitterStreamProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@EnableAutoConfiguration
public class Application {

    public static Map<String, AtomicInteger> cousumerCountMap;
    public static Rankings rankings;
    private static RollingCountTopologyRunner runner;

    @RequestMapping("/data")
    String data() {

        JSONObject data = new JSONObject();

        long total = 0;

        try {
            for (String thread : cousumerCountMap.keySet()) {
                AtomicInteger val = cousumerCountMap.get(thread);
                data.put("consumer"+thread, val);
                total+=val.get();
            }
            data.put("total", total);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return data.toString();
    }


    @RequestMapping("/rankings")
    String rankings() {

        //Rankings rankings = rankings.getRankings().

        //Collections.sort(, new RankingsComparator());

        JSONObject data = new JSONObject();

        if(null == rankings) {
            return "";
        }

        try {
            for (Rankable rankable : rankings.getRankings()) {
                data.put(rankable.getObject().toString(), String.valueOf(rankable.getCount()));
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return data.toString();
    }

    @RequestMapping("/start")
    String startStorm() {

        runner.runLocal();

        return "SUCCESS";
    }

    @RequestMapping("/stop")
    String stopStorm() {

        runner.stopLocal();

        return "SUCCESS";
    }

    @RequestMapping("/consume")
    String consumeStream() {

        TwitterStreamConsumer streamConsumer = new TwitterStreamConsumer();
        streamConsumer.consume();

        return "SUCCESS";
    }

    class RankingsComparator implements Comparator<Rankable> {
        @Override
        public int compare(Rankable a, Rankable b) {
            if(a.getCount() < b.getCount()) {
                return -1;
            } else if (a.getCount() > b.getCount()) {
                return 1;
            } else {
                return 1;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        cousumerCountMap = new HashMap<>();

        TwitterStreamProducer simpleProducer = new TwitterStreamProducer();
        simpleProducer.twitterStream();

        SpringApplication.run(Application.class, args);

        runner = new RollingCountTopologyRunner();

    }
}
