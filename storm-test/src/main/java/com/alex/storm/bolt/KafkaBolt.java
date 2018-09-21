package com.alex.storm.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class KafkaBolt extends BaseRichBolt {
    private static final Logger logger = Logger.getLogger(KafkaBolt.class);

    private OutputCollector collector;
    private static int num = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
//        String val = tuple.getStringByField("msg");
//        System.out.println(val);
//        String topic = tuple.getStringByField("topic");
//        String key = tuple.getStringByField("key");
//        String value = tuple.getStringByField("value");
//        System.out.println("topic: " + topic + " ;key: " + key + " ;value: " + value);
//        collector.ack(tuple);
        String val = tuple.getString(0);
        logger.info(num + " ---- " + val);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
