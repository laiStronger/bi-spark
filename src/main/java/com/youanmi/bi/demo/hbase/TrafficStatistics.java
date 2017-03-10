package com.youanmi.bi.demo.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author sunxiaolong
 */
public class TrafficStatistics {
    public static final String TABLE_NAME = "traffic_statistics";
    /**
     * 访问的url地址
     */
    private String location;
    /**
     * 访问的时间
     */
    private Long time;
    /**
     * 访问的次数
     */
    private Integer count;

    public TrafficStatistics() {

    }

    public TrafficStatistics(String location, Long time, Integer count) {
        this.location = location;
        this.time = time;
        this.count = count;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public void addToPut(Put put) {
        put.addColumn(Bytes.toBytes(Url.CF_NAME), Bytes.toBytes(Url.LOCATION), Bytes.toBytes(location));
        put.addColumn(Bytes.toBytes(Statistics.CF_NAME), Bytes.toBytes(Statistics.TIME), Bytes.toBytes(time));
        put.addColumn(Bytes.toBytes(Statistics.CF_NAME), Bytes.toBytes(Statistics.COUNT), Bytes.toBytes(count));
    }

    public boolean isValid() {
        return time != null && location != null && location.length() > 0;
    }


    public interface Url {
        String CF_NAME = "url";
        String LOCATION = "location";
    }

    public interface Statistics {
        String CF_NAME = "statistics";
        String TIME = "time";
        String COUNT = "count";
    }

}
