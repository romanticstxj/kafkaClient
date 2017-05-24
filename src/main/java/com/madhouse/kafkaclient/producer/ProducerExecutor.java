package com.madhouse.kafkaclient.producer;

import com.madhouse.kafkaclient.util.KafkaCallback;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.madhouse.kafkaclient.util.KafkaMessage;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public class ProducerExecutor implements Runnable {
    private KafkaProducer handle;
    private Producer producer;
    private ProducerConfig config;
    private KafkaCallback callback;

    ProducerExecutor(KafkaProducer handle, ProducerConfig config, KafkaCallback callback) {
        this.handle = handle;
        this.config = config;
        this.callback = callback;
        this.producer = new Producer(this.config);
    }

    @Override
    public void run() {
        List<KafkaMessage> messageQueue = null;

        while (!Thread.interrupted()) {
            try {
                messageQueue = this.handle.getMessageQueue();
                if (messageQueue != null && !messageQueue.isEmpty()) {
                    List<KeyedMessage> msgList = new LinkedList<>();

                    for (KafkaMessage msg : messageQueue) {
                        msgList.add(new KeyedMessage<String, String>(msg.topic, msg.key, msg.message));
                    }

                    this.producer.send(msgList);
                } else {
                    Thread.sleep(10);
                }
            } catch (Exception ex) {
                if (messageQueue != null && !messageQueue.isEmpty() && this.callback != null) {
                    this.callback.onSendError(messageQueue);
                }

                System.err.println(ex);
            }
        }
    }
}
