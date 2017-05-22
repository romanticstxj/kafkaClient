package com.madhouse.kafkaclient.util;

import java.nio.ByteBuffer;

/**
 * Created by WUJUNFENG on 2017/5/9.
*/
public abstract class KafkaCallback {
    public boolean onFetch(String topic, int partition, long offset, ByteBuffer message) {
        return true;
    }

    public void onSendError(String topic, String key, String message) {

    }
}
