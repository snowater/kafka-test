/*
 * @(#) HdfsTest.java 2016/10/31
 * 
 * Copyright 2013 snow.com, Inc. All rights reserved.
 */
package com.snow.hadoop;

import java.io.IOException;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author hzwanghuiqi
 * @version 2016/10/31
 */
public class HdfsTest {

    public static void main(String[] args) throws IOException {
        ApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"application-context.xml"});
        HdfsOperator hdfsOperator = context.getBean("hdfsOperator", HdfsOperator.class);
        
        // hdfsOperator.makeDir();
        hdfsOperator.writeFile();
        hdfsOperator.readFile();
        // hdfsOperator.copyFromLocalFile();
        // Path path = new Path("/home/hzwanghuiqi/Documents/hdfstest/");
        // hdfsOperator.getFiles(path);
        // hdfsOperator.deleteFile();
        // hdfsOperator.deleteDir();
    }

}
