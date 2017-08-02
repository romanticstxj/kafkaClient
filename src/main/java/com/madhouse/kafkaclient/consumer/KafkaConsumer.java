package com.madhouse.kafkaclient.consumer;

import com.madhouse.kafkaclient.util.KafkaCallback;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public class KafkaConsumer {
    private String brokers;
    private String groupId;
    private Properties props;
    private Map<String, ExecutorService> executorServiceMap;

    public KafkaConsumer(String brokers, String groupId) {
        this.brokers = brokers;
        this.groupId = groupId;

        this.props = new Properties();
        this.props.put("bootstrap.servers", this.brokers);
        this.props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.props.put("group.id", this.groupId);
        this.props.put("enable.auto.commit", false);
        this.props.put("auto.offset.reset", "earliest");
        this.props.put("heartbeat.interval.ms", 3000);
        this.props.put("session.timeout.ms", 30000);

        this.executorServiceMap = new HashMap<>();
    }

    public boolean start(String topic, KafkaCallback callback) {
        if (topic == null) {
            return false;
        }

        if (this.executorServiceMap.containsKey(topic)) {
            return false;
        }

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new ConsumerExecutor(this.props, this.groupId, topic, callback));

        this.executorServiceMap.put(topic, executorService);
        return true;
    }

    public void stop(String topic) {
        if (topic != null) {
            if (this.executorServiceMap.containsKey(topic)) {
                ExecutorService executorService = this.executorServiceMap.remove(topic);
                executorService.shutdown();
            }
        } else {
           for (Map.Entry<String, ExecutorService> entry : this.executorServiceMap.entrySet()) {
               entry.getValue().shutdown();
           }

            this.executorServiceMap.clear();
        }
    }
}
