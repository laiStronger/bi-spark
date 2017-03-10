package com.youanmi.bi.demo.handler;

import com.youanmi.bi.demo.hbase.TrafficStatistics;
import com.youanmi.bi.demo.utils.HbaseConnectionUtil;
import com.youanmi.bi.demo.utils.IDUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author sunxiaolong
 */
public class TrafficStatisticsHandler implements SparkTaskHandler<String, TrafficStatistics> {

    public Long parseTime(String timeStr) {
        SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd HH:mm:ss,SSS");
        try {
            return sdf.parse(timeStr).getTime();

        }
        catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void handleDStream(JavaDStream<String> dStream) {
        dStream
            .filter(str -> str.contains("DispatcherServlet with name 'springServlet' processing GET")
                    || str.contains("DispatcherServlet with name 'springServlet' processing POST"))
            .map(str -> {
                String dateTimeStr = str.substring(0, 21);
                Pattern p = Pattern.compile("\\[.*?\\]");
                Matcher m = p.matcher(str);
                String url = null;
                while (m.find()) {
                    str = m.group();
                    if (str.contains("/")) {
                        url = str.substring(1, str.length() - 1);
                    }
                }
                Long time = parseTime(dateTimeStr);

                return new TrafficStatistics(url, time, 1);
            }).filter(TrafficStatistics::isValid).foreachRDD(this::handleRDD);
    }

    @Override
    public void handleRDD(JavaRDD<TrafficStatistics> rdd) {
        rdd.foreachPartition(part -> {
            Connection connection = HbaseConnectionUtil.getConnection();
            Table table = connection.getTable(TableName.valueOf(TrafficStatistics.TABLE_NAME));
            ArrayList<Row> actions = new ArrayList<>();
            part.forEachRemaining(ts -> {
                Put put = new Put(Bytes.toBytes(IDUtils.generateIdWithTime(ts.getTime())));
                ts.addToPut(put);
                actions.add(put);
            });
            table.batch(actions, new Object[actions.size()]);
        });
    }
}
