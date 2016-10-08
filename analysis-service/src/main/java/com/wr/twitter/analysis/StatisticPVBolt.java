package com.wr.twitter.analysis;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: Alice
 * @email: chenzhangzju@yahoo.com
 * @date: 2016/10/8.
 */
//多线程统计每个访客对应的PV数
public class StatisticPVBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String date = tuple.getStringByField("date");
        String userId = tuple.getStringByField("sid");
        String key = date + "_" + userId;
        String urls = tuple.getStringByField("urls");
        Integer count = counts.get(key);
        if (count == null) {
            count = 0;
        }
        count += urls.split(",").length;
        counts.put(key, count);
        this.collector.emit(new Values(key, count));//每个访客  PV数
        this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("date_sid", "count"));
    }
}
