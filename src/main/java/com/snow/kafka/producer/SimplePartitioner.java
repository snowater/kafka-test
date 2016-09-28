/*
 * @(#) SimplePartitioner.java 2016/04/28
 * 
 * Copyright 2016 snow.com, Inc. All rights reserved.
 */
package com.snow.kafka.producer;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hzwanghuiqi
 * @version 2016/04/28
 */
public class SimplePartitioner implements Partitioner {
    public static final Logger logger = LoggerFactory.getLogger(SimplePartitioner.class);


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partition = 0;
        int offset = Integer.valueOf((String) key);
        if (offset >= 0) {
            partition = Integer.valueOf((String) key) % cluster.partitionCountForTopic(topic);
            // logger.info("key {}, partition {}", key, partition);
        }
        return partition;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void configure(Map<String, ?> configs) {
        // TODO Auto-generated method stub

    }
}
