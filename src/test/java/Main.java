import com.madhouse.kafkaclient.consumer.KafkaConsumer;
import com.madhouse.kafkaclient.producer.KafkaProducer;
import com.madhouse.kafkaclient.producer.ProducerExecutor;
import com.madhouse.kafkaclient.util.KafkaCallback;
import com.madhouse.kafkaclient.util.KafkaMessage;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */

public class Main {
    public static void main(String[] args) {
        String gourpId = "test";
        String topic = "test_hdfs";
        String brokers = "172.16.25.27:9092,172.16.25.28:9092,172.16.25.29:9092";

        KafkaConsumer consumer = new KafkaConsumer(brokers, gourpId);
        consumer.start(topic, new KafkaCallback() {
            @Override
            public boolean onFetch(String topic, int partition, long offset, byte[] message) {
                String msg = new String(message);
                System.out.println(String.format("%s-%d-%d-%s", topic, partition, offset, msg));
                return true;
            }
        });

        KafkaProducer producer = new KafkaProducer(brokers, 5, null);

        if (producer.start(new KafkaCallback() {
            @Override
            public void onCompletion(KafkaMessage message, Exception e) {
                if (e != null) {
                    System.out.println(e.toString());
                }
            }
        })) {
            long count = 0;

            try {
                while (true) {
                    String message = "tttttttt" + count;
                    producer.sendMessage(topic, message.getBytes());
                    count++;
                    Thread.sleep(100);
                }
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
        }
    }
}
