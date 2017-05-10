package com.madhouse.kafkaclient.producer;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */

import kafka.producer.ProducerConfig;
import com.madhouse.kafkaclient.util.KafkaMessage;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class KafkaProducer {
    private int maxThreadCount;
    private ExecutorService executorService;
    private List<KafkaMessage> messageQueue;
    private Properties props;
    private ProducerConfig config;
    private Logger logger = LogManager.getLogger(this.getClass());

    public KafkaProducer(String brokers, int maxBufferSize, int maxThreadCount, int acks, boolean asyncSend, boolean autoPartitioner) {

        this.maxThreadCount = maxThreadCount;

        this.props = new Properties();
        this.props.put("metadata.broker.list", brokers);
        this.props.put("send.buffer.bytes", Integer.toString(maxBufferSize));
        this.props.put("message.send.max.retries", "3");
        this.props.put("serializer.class", "kafka.serializer.StringEncoder");
        this.props.put("request.required.acks", Integer.toString(acks));
        this.props.put("batch.num.messages", "128");

        if (asyncSend) {
            this.props.put("producer.type", "async");
        } else {
            this.props.put("producer.type", "sync");
        }

        if (!autoPartitioner) {
            this.props.put("partitioner.class","com.madhouse.producer.ProducerPatitioner");
        }

        this.config = new ProducerConfig(this.props);

        this.messageQueue = new LinkedList<>();
        this.executorService = Executors.newFixedThreadPool(this.maxThreadCount);
    }

    public boolean start() {
        try {
            for (int i = 0; i < this.maxThreadCount; ++i) {
                this.executorService.submit(new ProducerExecutor(this, this.config));
            }
        } catch (Exception ex) {
            this.logger.error(ex);
            return false;
        }

        return true;
    }

    public boolean sendMessage(String topic, byte[] message) {
        return this.sendMessage(topic, null, new String(message));
    }

    public boolean sendMessage(String topic, String key, byte[] message) {
        return this.sendMessage(topic, key, new String(message));
    }

    public boolean sendMessage(String topic, String message) {
        return this.sendMessage(topic, null, message);
    }

    public boolean sendMessage(String topic, String key, String message) {
        try {
            synchronized (this) {
                if (this.messageQueue != null) {
                    this.messageQueue.add(new KafkaMessage(topic, key, message));
                } else {
                    return false;
                }
            }
        } catch (Exception ex) {
            this.logger.error(ex);
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
