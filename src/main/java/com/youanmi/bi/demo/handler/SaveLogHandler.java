package com.youanmi.bi.demo.handler;

import com.youanmi.bi.demo.hbase.Log;
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

/**
 * @author sunxiaolong
 */
public class SaveLogHandler implements SparkTaskHandler<String, Log> {

    public Long parseTime(String timeStr) {
        SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd HH:mm:ss,SSS");
        try {
            return sdf.parse(timeStr).getTime();

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void handleDStream(JavaDStream<String> dStream) {
        dStream
                .map(str -> {
                    Log log = new Log();
                    log.setContent(str);
                    log.setTime(parseTime(str.substring(0, 21)));
                    return log;
                })
                .foreachRDD(this::handleRDD);
    }

    @Override
    public void handleRDD(JavaRDD<Log> rdd) {
        rdd.foreachPartition(part -> {
            Connection connection = HbaseConnectionUtil.getConnection();
            Table table = connection.getTable(TableName.valueOf(Log.TABLE_NAME));
            ArrayList<Row> actions = new ArrayList<>();
            part.forEachRemaining(log -> {
                Put put = new Put(Bytes.toBytes(IDUtils.generateIdWithTime(log.getTime())));
                log.addToPut(put);
                actions.add(put);
            });
            table.batch(actions, new Object[actions.size()]);
        });
    }
}
