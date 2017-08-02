package com.madhouse.kafkaclient.producer;

import com.madhouse.kafkaclient.util.KafkaCallback;
import com.madhouse.kafkaclient.util.KafkaMessage;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Properties;


/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public class ProducerExecutor implements Runnable {
    private KafkaProducer handle;
    private org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> producer;
    private Properties props;
    private KafkaCallback callback;

    ProducerExecutor(KafkaProducer handle, Properties props, KafkaCallback callback) {
        this.handle = handle;
        this.props = props;
        this.callback = callback;
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(this.props);
    }

    @Override
    public void run() {
        List<KafkaMessage> messageQueue = null;

        while (!Thread.interrupted()) {
            try {
                messageQueue = this.handle.getMessageQueue();
                if (messageQueue != null && !messageQueue.isEmpty()) {

                    for (KafkaMessage msg : messageQueue) {
                        this.producer.send(new ProducerRecord<>(msg.topic, msg.key, msg.message), new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                callback.onCompletion(new KafkaMessage(msg.topic, msg.key, msg.message), e);
                            }
                        });
                    }
                } else {
                    Thread.sleep(10);
                }
            } catch (Exception ex) {
                if (messageQueue != null && !messageQueue.isEmpty() && this.callback != null) {
                    this.callback.onCompletion(null, ex);
                }

                System.err.println(ex);
            }
        }
    }
}
