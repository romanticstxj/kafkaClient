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
    private List<Pair<String, Integer>> brokers;
    private String groupId;
    private Map<String, ExecutorService> executorServiceMap;

    public KafkaConsumer(String brokers, String groupId) {
        this.brokers = new LinkedList<>();
        List<String> hosts = Arrays.asList(brokers.split(","));

        for (String host : hosts) {
            String[] addr = host.split(":");
            this.brokers.add(Pair.of(addr[0], Integer.parseInt(addr[1])));
        }

        this.groupId = groupId;
        this.executorServiceMap = new HashMap<>();
    }

    public boolean start(String topic, int partitions, int maxBufferSize, KafkaCallback callback) {
        if (topic == null || partitions <= 0 || maxBufferSize <= 0) {
            return false;
        }

        if (this.executorServiceMap.containsKey(topic)) {
            return false;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(partitions);
        for (int i = 0; i < partitions; ++i) {
            executorService.submit(new ConsumerExecutor(this.brokers, this.groupId, topic, i, maxBufferSize, callback));
        }

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
