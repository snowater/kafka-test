/*
 * @(#) ProducerMainTest.java 2016/09/23
 * 
 * Copyright 2016 snow.com, Inc. All rights reserved.
 */
package com.snow.kafka.producer;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

/**
 * @author hzwanghuiqi
 * @version 2016/09/23
 */
@Component
public class ProducerMainTest {

    public static void main(String[] args) {
        ApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"application-context.xml"});
        
        ProducerTest producerTest = context.getBean(ProducerTest.class);
        producerTest.sendMessage();
    }


}
