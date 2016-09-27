/*
 * @(#) ConsumerMainTest.java 2016/09/26
 * 
 * Copyright 2016 snow.com, Inc. All rights reserved.
 */
package com.snow.kafka.consumer;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

/**
 * @author hzwanghuiqi
 * @version 2016/09/26
 */
@Component
public class ConsumerMainTest {

    public static void main(String[] args) {
        ApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"application-context.xml"});
        ConsumerTest consumerTest = context.getBean(ConsumerTest.class);
        // consumerTest.simpleConsumer();
        // consumerTest.consumer();
        consumerTest.consumerExactly();
    }

}
