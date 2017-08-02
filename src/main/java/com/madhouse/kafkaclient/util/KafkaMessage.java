package com.madhouse.kafkaclient.util;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public class KafkaMessage {
    public String topic;
    public String key;
    public byte[] message;

    public KafkaMessage(String topic, String key, byte[] message) {
        this.topic = topic;
        this.key = key;
        this.message = message;
    }
}
