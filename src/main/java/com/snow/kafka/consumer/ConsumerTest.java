/*
 * @(#) ConsumerTest.java 2016/09/26
 * 
 * Copyright 2016 snow.com, Inc. All rights reserved.
 */
package com.snow.kafka.consumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author hzwanghuiqi
 * @version 2016/09/26
 */
@Component
public class ConsumerTest {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);

    // 在消费的时候生产者一定要处于运行状态，否则就会得不到数据，无法消费
    public Properties createSimpleProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "60000");
        properties.put("session.timeout.ms", "30000");
        properties.put("max.poll.records", "10000"); // 控制最大poll消息数量
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    // 自动提交偏移量
    public void simpleConsumer() {
        Properties properties = createSimpleProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("test"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(10);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("partition = {}, offset = {}, key = {}, value = {}", record.partition(), record.offset(), record.key(),
                                record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }


    public Properties createProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "60000");
        properties.put("session.timeout.ms", "30000");
        properties.put("max.poll.records", "10000"); // 控制最大poll消息数量
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    /**
     * 手动控制偏移量
     */
    public void consumer() {
        Properties properties = createProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(10);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("partition = {}, offset = {}, key = {}, value = {}", record.partition(), record.offset(), record.key(),
                                record.value());
                }
                if (!records.isEmpty()) {
                    // 异步提交offset
                    consumer.commitAsync(new OffsetCommitCallback() {

                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            for (Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
                                logger.info("commit offset: partition = {}, offset = {}", offset.getKey(), offset.getValue().offset());
                            }
                        }
                    });
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 手工精确控制每个分区的偏移量
     */
    public void consumerExactly() {
        Properties properties = createProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        logger.info("partition = {}, offset = {}, key = {}, value = {}", record.partition(), record.offset(), record.key(),
                                    record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }

    }

}
