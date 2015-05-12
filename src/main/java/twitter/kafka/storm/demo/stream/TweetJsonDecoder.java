package twitter.kafka.storm.demo.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TweetJsonDecoder implements Decoder<Object> {
    private static final Logger LOGGER = Logger.getLogger(TweetJsonDecoder.class);

    public TweetJsonDecoder(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }

    @Override
    public Object fromBytes(byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(bytes, String.class);
        } catch (IOException e) {
            LOGGER.error(String.format("Json processing failed for object: %s", bytes.toString()), e);
        }
        return null;
    }
}
