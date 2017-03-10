package com.youanmi.bi.spark.demo;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;


public class SparkSqlReadMySqlDemo {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local[2]").setAppName("sparkSql_mysql");

        SparkContext sparkContext = new SparkContext(sparkConf);

        // 创建sparkSession类
        SparkSession sparkSession = new SparkSession(sparkContext);

        SQLContext sqlContext = new SQLContext(sparkSession);

        String mysqlUrl = "jdbc:mysql://192.168.1.15:3306/o2o_test_33";

        String mysqlUser = "o2o";

        String mysqlPwd = "o2o";

        String table = "user_view";

        Properties mysqlProps = new Properties();
        // 设置账号密码
        mysqlProps.put("user", mysqlUser);
        mysqlProps.put("password", mysqlPwd);

        // 连接mysql并读取数据
        Dataset<Row> dataSet = sqlContext.read().jdbc(mysqlUrl, table, mysqlProps);

        // 把user_view表映射为内存中的hehe表
        dataSet.createOrReplaceTempView("hehe");

        // 查询id和user_name字段,条件为id>100和id<105,进行遍历每一条数据
        dataSet.select("id", "user_name").where("id > 100 and id < 105").foreach(new ForeachFunction<Row>() {

            public void call(Row t) throws Exception {
                // 打印数据
                System.out.println(t);
            }
        });

    }
}
