package com.madhouse.kafkaclient.producer;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */

import com.madhouse.kafkaclient.util.KafkaCallback;
import com.madhouse.kafkaclient.util.KafkaMessage;
import org.apache.kafka.clients.producer.Partitioner;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class KafkaProducer {
    private int maxThreadCount;
    private ExecutorService executorService;
    private List<KafkaMessage> messageQueue;
    private Properties props;

    public KafkaProducer(String brokers, int maxThreadCount, Partitioner partitioner) {

        this.maxThreadCount = maxThreadCount;

        this.props = new Properties();
        this.props.put("bootstrap.servers", brokers);
        this.props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.props.put("acks", "1");
        this.props.put("retries", 3);

        if (partitioner != null) {
            this.props.put("partitioner.class", partitioner.getClass().getName());
        }

        this.messageQueue = new LinkedList<>();
        this.executorService = Executors.newFixedThreadPool(this.maxThreadCount);
    }

    public boolean start(KafkaCallback callback) {
        try {
            if (callback != null && this.maxThreadCount > 0) {
                for (int i = 0; i < this.maxThreadCount; ++i) {
                    this.executorService.submit(new ProducerExecutor(this, this.props, callback));
                }
                return true;
            }
        } catch (Exception ex) {
            System.err.println(ex);
        }

        return false;
    }

    public boolean sendMessage(String topic, byte[] message) {
        return this.sendMessage(topic, Long.toString(System.currentTimeMillis()), message);
    }

    public boolean sendMessage(String topic, String key, byte[] message) {
        try {
            synchronized (this) {
                if (this.messageQueue != null) {
                    this.messageQueue.add(new KafkaMessage(topic, key, message));
                } else {
                    return false;
                }
            }
        } catch (Exception ex) {
            System.err.println(ex);
            return false;
        }

        return true;
    }

    public List<KafkaMessage> getMessageQueue() {
        List<KafkaMessage> queue = null;

        synchronized (this) {
            queue = this.messageQueue;
            this.messageQueue = new LinkedList<>();
        }

        return queue;
    }

    public void stop() {
        this.executorService.shutdown();
    }
}
