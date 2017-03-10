package com.youanmi.bi.demo.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author sunxiaolong
 */
public class Log {
    public static final String TABLE_NAME = "log";
    /**
     * 日志的时间
     */
    private Long time;
    /**
     * 日志的内容
     */
    private String content;

    public Long getTime() {
        return time;
    }

    /**
     * @param time
     */
    public void setTime(Long time) {
        this.time = time;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public void addToPut(Put put) {
        put.addColumn(Bytes.toBytes(Log.LogInfo.CF_NAME), Bytes.toBytes(Log.LogInfo.TIME), Bytes.toBytes(time));
        put.addColumn(Bytes.toBytes(Log.LogInfo.CF_NAME), Bytes.toBytes(Log.LogInfo.CONTENT), Bytes.toBytes(content));
    }

    public interface LogInfo {
        String CF_NAME = "log";
        String TIME = "time";
        String CONTENT = "content";
    }
}
