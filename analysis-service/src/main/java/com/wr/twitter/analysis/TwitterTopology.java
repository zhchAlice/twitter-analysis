package com.wr.twitter.analysis;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: Alice
 * @email: chenzhangzju@yahoo.com
 * @date: 2016/10/8.
 */
public class TwitterTopology {
    //配置zookeeper 主机号：端口号
    private static final String zks = "172.16.160.12:2181,172.16.160.13:2181,172.16.160.14:2181";
    //接受消息队列的主题
    private static final String topic = "twitter-source";
    //zookeeper设置文件中的配置，如果zookeeper配置文件中设置为主机名：端口号 ，该项为空
    private static final String zkRoot = "/twitter";

    public static final String SPOUT_ID = KafkaSpout.class .getSimpleName();
    public static final String CLIFORMATBOLT_ID = CliFormatBolt.class .getSimpleName();
    public static final String CLISUMBOLT_ID = CliSumBolt.class .getSimpleName();
    public static final String STATISTICPVBOLT_ID = StatisticPVBolt.class .getSimpleName();

    public static void main(String[] args) {
        //定义一个Topology 构造器
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, getKafkaSpout());
        builder.setBolt(CLIFORMATBOLT_ID, new CliFormatBolt()).shuffleGrouping(SPOUT_ID);
        builder.setBolt(STATISTICPVBOLT_ID, new StatisticPVBolt()).fieldsGrouping(CLIFORMATBOLT_ID,
                new Fields("date","sid"));
        builder.setBolt(CLISUMBOLT_ID, new CliSumBolt()).fieldsGrouping(STATISTICPVBOLT_ID, new Fields("date_sid"));

        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE , 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE , 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE , 16384);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TwitterTopology.class .getSimpleName(), conf , builder .createTopology());
    }

    private static KafkaSpout getKafkaSpout() {
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, "pv-uv");
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(spoutConfig);
    }
}
