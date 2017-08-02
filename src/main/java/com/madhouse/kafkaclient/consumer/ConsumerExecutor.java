package com.madhouse.kafkaclient.consumer;

import com.madhouse.kafkaclient.util.KafkaCallback;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public class ConsumerExecutor implements Runnable {
    private String brokers;
    private String topic;
    private String groupId;
    private Properties props;
    private long lastOffset;
    private KafkaCallback callback;
    private org.apache.kafka.clients.consumer.KafkaConsumer consumer;

    public ConsumerExecutor(String brokers, String groupId, String topic, KafkaCallback callback) {
        this.brokers = brokers;
        this.topic = topic;
        this.callback = callback;
        this.groupId = groupId;
        this.lastOffset = -1;

        this.props = new Properties();
        this.props.put("bootstrap.servers", this.brokers);
        this.props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.props.put("group.id", this.groupId);
        this.props.put("enable.auto.commit", false);
        this.props.put("auto.offset.reset", "earliest");
        this.props.put("heartbeat.interval.ms", 3000);
        this.props.put("session.timeout.ms", 30000);

        this.consumer = new KafkaConsumer(this.props);
    }

    @Override
    public void run() {
        this.consumer.subscribe(Collections.singletonList(this.topic));

        while (!Thread.interrupted()) {
            try {
                ConsumerRecords<String, byte[]> records = this.consumer.poll(500);
                for (ConsumerRecord record : records) {
                    if (this.callback.onFetch(record.topic(), record.partition(), record.offset(), (byte[]) record.value())) {
                        this.consumer.commitAsync();
                    }

                    this.lastOffset = record.offset();
                }
            } catch (Exception e) {
                System.err.println(e.toString());
                break;
            }
        }

        System.err.println(String.format("consumer groupid=[%s] topic=[%s] offset=[%d] executor exit.", this.groupId, this.topic, lastOffset));
    }

}