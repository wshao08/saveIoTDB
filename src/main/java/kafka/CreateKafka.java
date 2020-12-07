package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import static run.Launcher.p;


public class CreateKafka {

    private ConsumerConnector consumer;

    public CreateKafka() {
        Properties props = new Properties();

        /**
         * Zookeeper configuration
         */
        props.put("auto.offset.reset", "smallest");
        props.put("zookeeper.connect", "192.168.35.119:2181");
        props.put("group.id", "IoTDBTest0.11_1111");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("rebalance.max.retries", "5");
        props.put("rebalance.backoff.ms", "1200");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig config = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    public void consume() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("TYP_KTP_CTY_Decode", 9);

        /**
         * Specify data decoder
         */
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
            .createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("TYP_KTP_CTY_Decode");
        System.out.println(streams.size());
        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new KafkaConsumerThread(stream));
        }
    }

}
