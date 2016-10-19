/*
 * @(#) BeanSerializer.java 2016/10/11
 * 
 * Copyright 2013 NetEase.com, Inc. All rights reserved.
 */
package com.snow.kafka.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.alibaba.fastjson.JSON;

/**
 * 自定义Object反序列化方法
 * @author hzwanghuiqi
 * @version 2016/10/11
 */
public class BeanSerializer implements Serializer<Object> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // TODO Auto-generated method stub

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        // TODO Auto-generated method stub
        return JSON.toJSONBytes(data);
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
