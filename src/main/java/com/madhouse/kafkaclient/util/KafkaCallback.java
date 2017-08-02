package com.madhouse.kafkaclient.util;

import java.util.List;
import java.nio.ByteBuffer;

/**
 * Created by WUJUNFENG on 2017/5/9.
*/
public abstract class KafkaCallback {
    public boolean onFetch(String topic, int partition, long offset, byte[] message) {
        return true;
    }

    public void onCompletion(KafkaMessage message, Exception e) {

    }
}
