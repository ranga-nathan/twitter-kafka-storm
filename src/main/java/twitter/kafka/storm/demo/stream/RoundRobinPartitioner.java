package twitter.kafka.storm.demo.stream;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinPartitioner implements Partitioner {

    private AtomicInteger counter = new AtomicInteger(0);

    public RoundRobinPartitioner() {
    }

    public RoundRobinPartitioner(VerifiableProperties props) {
    }

    @Override
    public int partition(Object key, int numPartitions) {
        int result = counter.incrementAndGet() % numPartitions;
        if (counter.get() > 65536) {
            counter.set(0);
        }
        return result;
    }
}
