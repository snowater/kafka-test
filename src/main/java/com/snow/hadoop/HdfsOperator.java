/*
 * @(#) HdfsTest.java 2016/10/31
 * 
 * Copyright 2013 snow.com, Inc. All rights reserved.
 */
package com.snow.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author snow
 * @version 2016/10/31
 */
@Component(value = "hdfsOperator")
public class HdfsOperator {
    private static final Logger logger = LoggerFactory.getLogger(HdfsOperator.class);
    private String pathString = "/home/hzwanghuiqi/Documents/hdfstest";

    public void makeDir() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathString);
        fs.mkdirs(path);
        fs.close();
    }
    
    public void deleteDir() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathString);
        fs.delete(path, true);
        fs.close();
    }
    
    public void writeFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathString + "/test");
        FSDataOutputStream out = fs.create(path);
        out.writeBytes("hello world\n");
        out.close();
        fs.close();
    }

    public void readFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathString + "/test");
        if (fs.exists(path)) {
            FSDataInputStream in = fs.open(path);
            FileStatus status = fs.getFileStatus(path);
            byte[] buffer = new byte[(int) status.getLen()];
            in.readFully(0, buffer);
            in.close();
            fs.close();
            logger.info("read : {}", new String(buffer));
        }
    }

    public void deleteFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathString + "/test");
        fs.delete(path, true); // 删除文件设置为false或true都可以
        fs.close();
    }

    public void copyFromLocalFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path("/home/hzwanghuiqi/Documents/rediskey");
        Path dstPath = new Path(pathString);
        fs.copyFromLocalFile(srcPath, dstPath);
        fs.close();
    }

    // 遍历某个目录及其子目录
    public void getFiles(Path path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(path);
        for (FileStatus status : fileStatus) {
            if (status.isDirectory()) {
                Path tmpPath = status.getPath();
                getFiles(tmpPath); // 递归调用
            } else {
                logger.info("{}", status.getPath().getName());
            }
        }
    }
}


