package com.youanmi.bi.demo.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


/**
 * @author sunxiaolong
 */
public class HbaseConnectionUtil {

    private static Connection connection = null;

    private HbaseConnectionUtil() {
    }

    public static Connection getConnection() {
        if (connection == null) {
            synchronized (HbaseConnectionUtil.class) {
                if (connection == null) {
                    Configuration conf = HBaseConfiguration.create();
                    conf.addResource(new Path("hbase-site.xml"));
                    conf.addResource(new Path("core-site.xml"));
                    try {
                        connection = ConnectionFactory.createConnection(conf);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return connection;
    }
}
