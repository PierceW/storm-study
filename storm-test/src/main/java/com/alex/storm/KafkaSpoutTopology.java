package com.alex.storm;

import com.alex.storm.bolt.KafkaBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class KafkaSpoutTopology {

    private static String bootstrapServers = "192.168.33.86:9092";
    private static String topic = "pub_visit_topic";
    private static String TOPIC_0_1_STREAM = "pub_visit_topic_stream";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

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

        KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig.builder(bootstrapServers, topic)
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST)
                .setGroupId("test_kafka_spout092112")
                .setRecordTranslator(trans)
                .build();


        builder.setSpout("kafka_reader", new KafkaSpout<>(kafkaSpoutConfig), 2);
        builder.setBolt("kafka_bole", new KafkaBolt(), 4).shuffleGrouping("kafka_reader", TOPIC_0_1_STREAM);

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);
        StormSubmitter.submitTopology("get_kafka", config, builder.createTopology());
    }
}
