package com.wr.twitter.analysis;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: Alice
 * @email: chenzhangzju@yahoo.com
 * @date: 2016/10/8.
 */
public class CliSumBolt extends BaseRichBolt {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private OutputCollector collector;
    private Map<String, Integer> counts = new HashMap<String, Integer>();
    private HBaseUtils hBaseUtils = new HBaseUtils();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        int pv = 0;
        int uv = 0;

        String dateSid = tuple.getStringByField("date_sid");
        Integer count = tuple.getIntegerByField("count");
        String currDate = dateFormat.format(new Date());
        preHandle(dateSid.split("_")[0],currDate);

        counts.put(dateSid, count);
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            if(dateSid.split("_")[0].startsWith(currDate)){
                uv++;
                pv+=e.getValue();
            }
        }
        System.out.println(currDate + "的pv数为"+pv+",uv数为"+uv);
        put(pv, uv, currDate);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    /**
     * 预处理：访问日期若不是以今天开头并且访问日期大于当前日期,则计算新的一天的UV
     * @param date
     * @param currDate
     */
    private void preHandle(String date, String currDate) {

        Date accessDate = null;
        try {
            accessDate = dateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if (!date.startsWith(currDate) && accessDate.after(new Date())) {
            counts.clear();
        }
    }

    public void put(int pv, int uv, String currDate) {
        try {
            hBaseUtils.insertRow("pv_uv", "info", "pv", currDate, String.valueOf(pv).getBytes());
            hBaseUtils.insertRow("pv_uv", "info", "uv", currDate, String.valueOf(uv).getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
