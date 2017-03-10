package com.youanmi.bi.demo;

import com.youanmi.bi.demo.handler.SaveLogHandler;
import com.youanmi.bi.demo.handler.SparkTaskHandler;
import com.youanmi.bi.demo.handler.TrafficStatisticsHandler;
import com.youanmi.bi.demo.utils.PropertyUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

import java.util.ArrayList;
import java.util.List;


/**
 * @author sunxiaolong
 */
public class Start {

    private static List<SparkTaskHandler<String, ?>> dStreamHandlers = new ArrayList<>();

    static{
        dStreamHandlers.add(new TrafficStatisticsHandler());
        dStreamHandlers.add(new SaveLogHandler());
    }

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(PropertyUtils.getString("spark.master"));
        sparkConf.setAppName(PropertyUtils.getString("app.name"));
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Seconds.apply(PropertyUtils.getInteger("streaming.interval")));
        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(streamingContext, PropertyUtils.getString("flume.host"), Integer.valueOf(PropertyUtils.getString("flume.port")));

        JavaDStream<String> dStream = flumeStream.map(e -> new String(e.event().getBody().array(), "utf-8"));
        dStreamHandlers.forEach(handler -> handler.handleDStream(dStream));
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
