package com.wr.twitter.analysis;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * @author: Alice
 * @email: chenzhangzju@yahoo.com
 * @date: 2016/10/8.
 */
public class CliFormatBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static final String DATE_FORMATE =  "yyyy-MM-dd";
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String msg = tuple.getString(0);
        JSONObject object = parseJsonString(msg);
        if (null == object) {
            return;
        }
        //获取时间戳(Unix时间戳)，并转化为指定日期格式
        String date = TimeStamp2Date(object.get("timestamp").toString(),DATE_FORMATE);
        String userId = object.get("user_id").toString();
        String urls = object.get("urls").toString();
        this.collector.emit(new Values(date, userId, urls));
        this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("date","sid", "urls"));
    }

    /**
     * 解析json字符串
     * @param str
     * @return
     */
    public JSONObject parseJsonString(String str) {
        JSONParser parser = new JSONParser();
        JSONObject object = null;
        try {
            object = (JSONObject)parser.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if (null == object || !object.containsKey("timestamp") || !object.containsKey("user_id")
                || !object.containsKey("urls")) {
            return null;
        }
        return object;
    }

    public String TimeStamp2Date(String timestampString, String formats){
        Long timestamp = Long.parseLong(timestampString)*1000;
        String date = new SimpleDateFormat(formats).format(new java.util.Date(timestamp));
        return date;
    }
}
