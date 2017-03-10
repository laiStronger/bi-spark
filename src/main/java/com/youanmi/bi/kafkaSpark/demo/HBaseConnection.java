package com.youanmi.bi.kafkaSpark.demo;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.json.JSONObject;

public class HBaseConnection {

    private static final Log LOG = LogFactory.getLog(HBaseConnection.class);

    private static Connection connection;

    public static Connection getInstance() throws IOException {
        if (connection == null) {

            LOG.info("init hbase connection");

            Configuration configuration = HBaseConfiguration.create();

            configuration.set("hbase.zookeeper.quorum", "192.168.1.30");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }

    public static void insertUserData(String tableName, String family,
                                      Map<String, Object> dataMap) throws IOException {

        Table table = getInstance().getTable(TableName.valueOf(tableName));

        Put put = new Put(dataMap.get("id").toString().getBytes());

        dataMap.remove("id");

        for (String key : dataMap.keySet()) {
            put.addColumn(family.getBytes(), key.getBytes(), dataMap.get(key)
                    .toString().getBytes());
        }

        table.put(put);

        table.close();
    }

    public static void main(String[] args) throws IOException {
        Random random = new Random();
        JSONObject json = new JSONObject();
        int pronvinceId = random.nextInt(KafkaProducerDemo.PROVINCES.length);
        json.put("provinceId", pronvinceId);
        json.put("province", KafkaProducerDemo.PROVINCES[pronvinceId]);
        json.put("sex", KafkaProducerDemo.SEXS[random.nextInt(2)]);
        json.put("age", random.nextInt(80));
        json.put("time", System.currentTimeMillis());
        json.put("id", System.currentTimeMillis());
        System.out.println(json.get("id"));
        insertUserData("account:user", "address", json.toMap());

    }

}
