package com.youanmi.bi.demo.handler;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;

/**
 * @author sunxiaolong
 */
public interface SparkTaskHandler<T,K> extends Serializable{
    void handleDStream(JavaDStream<T> dStream);

    void handleRDD(JavaRDD<K> rdd);
}
