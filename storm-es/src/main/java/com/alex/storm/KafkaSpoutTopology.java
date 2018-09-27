package com.alex.storm;

import com.alex.storm.bolt.EsIndexBolt;
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
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

import java.util.HashMap;
import java.util.Map;

public class KafkaSpoutTopology {

    private static String bootstrapServers = "192.168.33.86:9092";
    private static String topic = "pub_visit_topic";
    private static String TOPIC_0_1_STREAM = "pub_visit_topic_stream";
    private static String ES_INSERT_STREAM = "es_insert_stream";

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
                .setGroupId("test_kafka_spout092716")
                .setRecordTranslator(trans)
                .build();

        builder.setSpout("kafka_reader", new KafkaSpout<>(kafkaSpoutConfig), 2);
        builder.setBolt("kafka_bolt", new KafkaBolt(), 4).shuffleGrouping("kafka_reader", TOPIC_0_1_STREAM);

        // elasticsearch 写入
        Map<String, Object> additionalParameters = new HashMap<>();
        additionalParameters.put("client.transport.sniff", "true");
        additionalParameters.put(ConfigurationOptions.ES_NODES, "192.168.33.151");
        additionalParameters.put(ConfigurationOptions.ES_BATCH_FLUSH_MANUAL_DEFAULT, true);
//        EsConfig esConfig = new EsConfig("my-application", new String[]{"192.168.34.56:9300"});
//        EsTupleMapper tupleMapper = new DefaultEsTupleMapper();
//        builder.setBolt("kafka_es_bolt", new EsIndexBolt(esConfig, tupleMapper), 4).shuffleGrouping("kafka_bolt", ES_INSERT_STREAM);
        builder.setBolt("kafka_es_bolt", new EsIndexBolt("pub_visit_topic/pub_visit_topic", additionalParameters)).shuffleGrouping("kafka_bolt", ES_INSERT_STREAM);

        Config config = new Config();
        config.setDebug(false);
        config.put("es.index.auto.create", "true");


        config.setNumWorkers(2);
        StormSubmitter.submitTopology("get_kafka", config, builder.createTopology());
    }
}
