package com.wr.twitter.analysis;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author: Alice
 * @email: chenzhangzju@yahoo.com
 * @date: 2016/10/8.
 */
public class CliFormatBoltTest {
    //private CliFormatBolt bolt = new CliFormatBolt();

    private static final String DATE_FORMATE =  "yyyy-MM-dd";
    @Test
    public void parseJsonString() {
        CliFormatBolt bolt = new CliFormatBolt();
        String jsonString = "{\"timestamp\": 1351742400, \"hashtags\": [\"gilenya\", \"MS\"], \"user_id\": " +
                "\"81768102\", \"urls\": [\"http://iamdavecarey.com\", \"http://www.baidu.com\"]}";
        JSONObject object= bolt.parseJsonString(jsonString);
        if (null != object) {
            String date = bolt.TimeStamp2Date(object.get("timestamp").toString(),DATE_FORMATE);
            System.out.println("time:" + date + "\n");;
            String userId = object.get("user_id").toString();
            String urls = object.get("urls").toString();
            System.out.println("userId:" + userId + "\n");
            System.out.println("urls:" + urls + "\n");
        }

    }
}