package com.madhouse.kafkaclient.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.madhouse.kafkaclient.util.KafkaMessage;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.apache.logging.log4j.*;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public class ProducerExecutor implements Runnable {
    private KafkaProducer handle;
    private Producer producer;
    private ProducerConfig config;
    private Properties props;
    private Logger logger = LogManager.getLogger(this.getClass());

    ProducerExecutor(KafkaProducer handle, ProducerConfig config) {
        this.handle = handle;
        this.config = config;
        this.producer = new Producer(this.config);
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                List<KafkaMessage> messageQueue = this.handle.getMessageQueue();
                if (messageQueue != null && messageQueue.size() > 0) {
                    List<KeyedMessage> msgList = new LinkedList<>();

                    for (KafkaMessage msg : messageQueue) {
                        msgList.add(new KeyedMessage<String, String>(msg.topic, msg.key, msg.message));
                    }

                    this.producer.send(msgList);
                } else {
                    Thread.sleep(10);
                }
            } catch (Exception ex) {
                this.logger.error(ex);
            }
        }
    }
}
