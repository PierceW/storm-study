package com.alex.storm;

import com.alex.storm.bolt.KafkaBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class KafkaSpoutTopologyMainNamed {

    private static final String BROKER_URL = "192.168.33.86:9092";
    private static final String TOPIC_0_1_STREAM = "test_0_1_stream";
    private static final String TOPIC_0_2_STREAM = "test_0_2_stream";
    private static final String TOPIC_0 = "pub_visit_topic";
    private static final String TOPIC_1 = "pub_visit_topic_event";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
            new KafkaSpoutTopologyMainNamed().runMain(args);
    }

    private void runMain(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        final String brokerUrl = args.length > 0 ? args[0] : BROKER_URL;

        Config config = getConfig();

        StormSubmitter.submitTopology("kafka-spout", config, getTopologyKafkaSpout(getKafkaSpoutConfig(brokerUrl)));
    }

    private KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
                (r) -> {
                    try {
                        return new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value());
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                },
                new Fields("topic", "partition", "offset", "key", "value"), TOPIC_0_1_STREAM);

        trans.forTopic(TOPIC_1,
                (r) -> {
                    try {
                        return new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value());
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                },
                new Fields("topic", "partition", "offset", "key", "value"), TOPIC_0_2_STREAM);

        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{TOPIC_0})
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                .setProp("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//                .setProp("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .setRetry(getRetryService())
                .setRecordTranslator(trans)
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    protected KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

    private StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> kafkaSpoutConfig) {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout<>(kafkaSpoutConfig), 1);
        builder.setBolt("kakfa_bolt", new KafkaBolt())
                .shuffleGrouping("kafka_spout", TOPIC_0_1_STREAM)
                .shuffleGrouping("kafka_spout", TOPIC_0_2_STREAM);
        return builder.createTopology();
    }

    private Config getConfig() {
        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);
        return config;
    }
}
