package com.madhouse.kafkaclient.producer;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */

import com.madhouse.kafkaclient.util.KafkaCallback;
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
    private KafkaCallback callback;
    private boolean autoPartitioner;
    private Logger logger = LogManager.getLogger(this.getClass());

    public KafkaProducer(String brokers, int maxBufferSize, int maxThreadCount, boolean autoPartitioner, KafkaCallback callback) {

        this.maxThreadCount = maxThreadCount;
        this.callback = callback;

        this.props = new Properties();
        this.props.put("metadata.broker.list", brokers);
        this.props.put("send.buffer.bytes", Integer.toString(maxBufferSize));
        this.props.put("message.send.max.retries", "3");
        this.props.put("serializer.class", "kafka.serializer.StringEncoder");
        this.props.put("request.required.acks", "1");
        this.props.put("batch.num.messages", "1024");
        this.props.put("queue.buffering.max.ms", "100");
        this.props.put("producer.type", "async");

        if (!(this.autoPartitioner = autoPartitioner)) {
            this.props.put("partitioner.class","com.madhouse.kafkaclient.producer.ProducerPatitioner");
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
        return this.sendMessage(topic, this.autoPartitioner ? Long.toString(System.currentTimeMillis()) : null, new String(message));
    }

    public boolean sendMessage(String topic, String key, byte[] message) {
        return this.sendMessage(topic, key, new String(message));
    }

    public boolean sendMessage(String topic, String message) {
        return this.sendMessage(topic, this.autoPartitioner ? Long.toString(System.currentTimeMillis()) : null, message);
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

    public KafkaCallback getCallback() {
        return this.callback;
    }
}
