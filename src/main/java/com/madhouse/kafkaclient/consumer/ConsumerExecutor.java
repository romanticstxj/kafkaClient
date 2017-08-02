package com.madhouse.kafkaclient.consumer;

import com.madhouse.kafkaclient.util.KafkaCallback;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public class ConsumerExecutor implements Runnable {
    private String topic;
    private String groupId;
    private Properties props;
    private long lastOffset;
    private KafkaCallback callback;
    private org.apache.kafka.clients.consumer.KafkaConsumer consumer;

    public ConsumerExecutor(Properties props, String groupId, String topic, KafkaCallback callback) {
        this.props = props;
        this.groupId = groupId;
        this.topic = topic;
        this.callback = callback;
        this.lastOffset = -1;

        this.consumer = new KafkaConsumer(this.props);
    }

    @Override
    public void run() {
        this.consumer.subscribe(Collections.singletonList(this.topic));

        while (!Thread.interrupted()) {
            try {
                ConsumerRecords<String, byte[]> records = this.consumer.poll(500);
                for (ConsumerRecord record : records) {
                    if (this.callback.onFetch(record.topic(), record.partition(), record.offset(), (byte[])(record.value()))) {
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