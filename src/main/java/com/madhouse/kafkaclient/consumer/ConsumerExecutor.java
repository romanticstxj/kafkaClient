package com.madhouse.kafkaclient.consumer;

import com.madhouse.kafkaclient.util.KafkaCallback;
import com.madhouse.kafkaclient.util.KeyValuePair;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public class ConsumerExecutor implements Runnable {
    private List<KeyValuePair<String, Integer>> brokers;
    private String topic;
    private int partition;
    private int soTimeout;
    private int fetchSize;
    private int maxBufferSize;
    private int maxRetryTimes;
    private String groupId;
    private String clientName;
    private KafkaCallback callback;
    private Logger logger = LogManager.getLogger(this.getClass());

    public ConsumerExecutor(List<KeyValuePair<String, Integer>> brokers, String groupId, String topic, int partition, int maxBufferSize, KafkaCallback callback) {
        this.brokers = brokers;
        this.topic = topic;
        this.soTimeout = 30000;
        this.maxRetryTimes = 3;
        this.fetchSize = 1024;
        this.maxBufferSize = maxBufferSize;
        this.partition = partition;
        this.callback = callback;
        this.groupId = groupId;
        this.clientName = String.format("%s-%s-%d", this.groupId, this.topic, this.partition);
    }

    @Override
    public void run() {
        int retryTimes = 0;
        long lastOffset = 0;
        SimpleConsumer consumer = null;

        while (!Thread.interrupted()) {
            try {
                if (consumer == null) {
                    KeyValuePair<String, Integer> leader = this.findLeader();
                    if (leader != null) {
                        retryTimes = 0;
                        consumer = new SimpleConsumer(leader.first, leader.sencond, this.soTimeout, this.maxBufferSize, this.clientName);
                        if ((lastOffset = this.getLastOffset(consumer)) <= 0) {
                            lastOffset = this.getLastOffset(consumer, kafka.api.OffsetRequest.EarliestTime());
                        }
                    } else {
                        if (++retryTimes > this.maxRetryTimes) {
                            return;
                        }

                        Thread.sleep(1000);
                        continue;
                    }
                }

                FetchRequest req = new FetchRequestBuilder()
                        .clientId(clientName)
                        .addFetch(topic, partition, lastOffset, this.fetchSize)
                        .build();

                FetchResponse resp = consumer.fetch(req);

                if (resp.hasError()) {
                    short code = resp.errorCode(topic, partition);
                    if (code != ErrorMapping.OffsetOutOfRangeCode()) {
                        consumer.close();
                        return;
                    }

                    lastOffset = this.getLastOffset(consumer, kafka.api.OffsetRequest.LatestTime());
                } else {
                    long newOffset = lastOffset;
                    for (MessageAndOffset msgAndOffset : resp.messageSet(topic, partition)) {
                        if (!this.callback.onRecv(this.topic, this.partition, msgAndOffset.offset(), msgAndOffset.message().payload())) {
                            consumer.close();
                            return;
                        }

                        newOffset = msgAndOffset.nextOffset();
                    }

                    if (newOffset > lastOffset) {
                        lastOffset = newOffset;
                        this.updateLastOffset(consumer, this.topic, this.partition, lastOffset, this.groupId, this.clientName);
                    } else {
                        Thread.sleep(10);
                    }
                }
            } catch (Exception ex) {
                this.logger.error(ex);
            }
        }

        if (consumer != null) {
            consumer.close();;
        }
    }

    private KeyValuePair<String, Integer> findLeader() {
        for (KeyValuePair<String, Integer> broker : brokers) {
            try {
                SimpleConsumer consumer = new SimpleConsumer(broker.first, broker.sencond, this.soTimeout, this.maxBufferSize, "leaderLookup");
                TopicMetadataRequest req = new TopicMetadataRequest(Collections.singletonList(this.topic));
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata topicMeta : metaData) {
                    for (kafka.javaapi.PartitionMetadata partitionMeta : topicMeta.partitionsMetadata()) {
                        if (partitionMeta.partitionId() == this.partition && partitionMeta.leader() != null) {
                            return new KeyValuePair<>(partitionMeta.leader().host(), partitionMeta.leader().port());
                        }
                    }
                }
            } catch (Exception ex) {
                this.logger.error(ex);
            }
        }

        return null;
    }

    private long getLastOffset(SimpleConsumer consumer) {
        TopicAndPartition topicInfo = new TopicAndPartition(topic, partition);
        List<TopicAndPartition> requestInfo = Collections.singletonList(topicInfo);
        OffsetFetchRequest req = new OffsetFetchRequest(this.groupId, requestInfo, 0, clientName);
        OffsetFetchResponse resp = consumer.fetchOffsets(req);

        Map<TopicAndPartition, OffsetMetadataAndError> metaData = resp.offsets();

        if (metaData != null && !metaData.isEmpty()) {
            OffsetMetadataAndError offsetMeta = metaData.get(topicInfo);
            if (offsetMeta != null && offsetMeta.error() == ErrorMapping.NoError()) {
                return offsetMeta.offset();
            }
        }

        this.logger.warn("get groupid=[{}] topic=[{}] partition=[{}] last offset error.", this.groupId, this.topic, this.partition);
        return 0;
    }

    public long getLastOffset(SimpleConsumer consumer, long whichTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest req = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse resp = consumer.getOffsetsBefore(req);

        if (!resp.hasError()) {
            long[] offsets = resp.offsets(topic, partition);
            return offsets[0];
        }

        this.logger.warn("get groupid=[{}] topic=[{}] partition=[{}] last offset error.", this.groupId, this.topic, this.partition);
        return 0;
    }

    public boolean updateLastOffset(SimpleConsumer consumer, String topic, int partition, long offset, String groupId, String clientName) {
        Map<TopicAndPartition, OffsetAndMetadata> requestInfo = new HashMap<>();
        TopicAndPartition topicInfo = new TopicAndPartition(topic, partition);
        requestInfo.put(topicInfo, new OffsetAndMetadata(offset, OffsetAndMetadata.NoMetadata(), -1));
        kafka.javaapi.OffsetCommitRequest req = new OffsetCommitRequest(groupId, requestInfo, 0, clientName);
        kafka.javaapi.OffsetCommitResponse resp = consumer.commitOffsets(req);

        if (!resp.hasError()) {
            return true;
        }

        this.logger.error("update groupid=[{}] topic=[{}] partition=[{}] offset=[{}] last offset error.", this.groupId, this.topic, this.partition, offset);
        return false;
    }
}
