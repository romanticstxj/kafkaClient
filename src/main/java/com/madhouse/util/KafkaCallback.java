package com.madhouse.util;

import java.nio.ByteBuffer;

/**
 * Created by WUJUNFENG on 2017/5/9.
*/
public interface KafkaCallback {
    public abstract boolean onRecv(String topic, int partition, long offset, ByteBuffer message);
}
