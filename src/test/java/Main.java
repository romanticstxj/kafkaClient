import com.madhouse.kafkaclient.consumer.KafkaConsumer;
import com.madhouse.kafkaclient.producer.KafkaProducer;
import com.madhouse.kafkaclient.util.KafkaCallback;
import com.madhouse.kafkaclient.util.KafkaMessage;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public class Main {
    public static void main(String[] args) {

        KafkaConsumer consumer = new KafkaConsumer("172.16.25.169:9092,172.16.25.180:9092,172.16.25.181:9092", "test");

        consumer.start("test", 7, 102400, new KafkaCallback() {
            @Override
            public boolean onFetch(String topic, int partition, long offset, ByteBuffer message) {
                byte[] buffer = new byte[message.limit()];
                message.get(buffer);
                String msg = new String(buffer);
                System.out.println(String.format("%s-%d-%d-%s", topic, partition, offset, msg));
                return true;
            }
        });

/*        KafkaProducer producer = new KafkaProducer("172.16.25.169:9092,172.16.25.180:9092,172.16.25.181:9092", 102400, 5, null);

        long count = 0;
        String message = "tttttttt";

        if (producer.start(new KafkaCallback() {
            @Override
            public void onSendError(List<KafkaMessage> message) {
                super.onSendError(message);
            }
        })) {
            try {
                while (true) {
                    producer.sendMessage("test", message + count);
                    count++;
                    Thread.sleep(1000);
                }
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
        }*/

    }
}
