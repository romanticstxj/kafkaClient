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
    private KafkaCallback callback;
    private org.apache.kafka.clients.consumer.KafkaConsumer consumer;

    public ConsumerExecutor(Properties props, String groupId, String topic, KafkaCallback callback) {
        this.props = props;
        this.groupId = groupId;
        this.topic = topic;
        this.callback = callback;

        this.consumer = new KafkaConsumer(this.props);
    }

    @Override
    public void run() {
        this.consumer.subscribe(Collections.singletonList(this.topic));

        while (!Thread.interrupted()) {
            try {
                boolean commit = true;
                ConsumerRecords<String, byte[]> records = this.consumer.poll(500);
                for (ConsumerRecord record : records) {
                    commit = commit && this.callback.onFetch(record.topic(), record.partition(), record.offset(), (byte[])(record.value()));
                }

                if (!records.isEmpty() && commit) {
                    this.consumer.commitAsync();
                }
            } catch (Exception e) {
                System.err.println(e.toString());
                break;
            }
        }

        System.err.println(String.format("consumer groupid=[%s] topic=[%s] executor exit.", this.groupId, this.topic));
    }

}