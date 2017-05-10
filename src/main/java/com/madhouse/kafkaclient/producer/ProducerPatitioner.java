package com.madhouse.kafkaclient.producer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import sun.security.provider.VerificationProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public class ProducerPatitioner implements Partitioner {
    public ProducerPatitioner(VerifiableProperties props) { };

    @Override
    public int partition(Object key, int numPartitions) {
        return 0;
    }
}
