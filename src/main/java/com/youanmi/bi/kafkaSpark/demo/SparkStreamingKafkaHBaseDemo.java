package com.youanmi.bi.kafkaSpark.demo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.DateFormatClass;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.LongAccumulator;
import org.json.JSONObject;

import scala.Tuple2;

import com.youanmi.bi.spark.bean.User;
import com.youanmi.bi.spark.hbase.HBaseDAO;


/**
 * spark streaming示例,接受kafka的数据,存储到hbase,然后用spark SQL计算
 * 
 * @author tanguojun
 *
 */
public class SparkStreamingKafkaHBaseDemo {

    public static void main(String[] args) throws InterruptedException {

        String appName = "user_collect";

        System.out.println("start spark streaming . app name = " + appName);

        // 创建sparkConf,spark streaming运行最新cpu需求为2,一个cpu接受数据,一个cpu运行任务
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName(appName);

        // SparkConf sparkConf = new
        // SparkConf().setMaster(args[0]).setAppName(args[1]);

        // 每20秒执行一次任务
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, new Duration(20000L));

        String zkAddress = "192.168.1.6:2181";

        // kafka分组
        String kafkaGroup = "a_group";

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        // 设置topic
        topicMap.put(KafkaProducerDemo.TOPIC, 2);

        // 创建kafka接受数据源
        JavaPairReceiverInputDStream<String, String> javaPairInputDStream =
                KafkaUtils.createStream(javaStreamingContext, zkAddress, kafkaGroup, topicMap);

        // System.out.println("rowKey的前缀为:" + date);
        // 定义spark广播变量,rowkey前缀
        // final Broadcast<String> rowKeyPrefixBroad =
        // javaStreamingContext.sparkContext().broadcast(date);

        // spark累加器,定义了此累加器可以在并行任务中使用
        final LongAccumulator messageCount =
                javaStreamingContext.sparkContext().sc().longAccumulator("messageCount");
        // 设置累加器初始值
        messageCount.setValue(0);

        JavaDStream<String> lines = javaPairInputDStream.map(new Function<Tuple2<String, String>, String>() {

            public String call(Tuple2<String, String> v1) throws Exception {

                // 累加器加1
                messageCount.add(1);
                System.out.println("messageCount:" + messageCount.value());

                // 接受到kafka的数据,这里给数据生成了一个rowKey,使用广播变量+累加器的值
                return new JSONObject(v1._2()).put(
                    "rowKey",
                    DateFormatUtils.format(System.currentTimeMillis(), "yyyyMMddHHmmssSSS") + "_"
                            + messageCount.value()).toString();
            }
        });

        // 遍历数据,存储数据到hbase
        lines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            public void call(JavaRDD<String> v1, Time v2) throws Exception {
                v1.foreach(new VoidFunction<String>() {
                    public void call(String t) throws Exception {

                        JSONObject data = new JSONObject(t);
                        // 将json转换为javabean
                        User user =
                                new User(data.getString("rowKey"), data.getInt("provinceId"), data
                                    .getString("province"), data.getString("sex"), data.getLong("time"));

                        // 新增数据到hbase,这里hbase一定要使用单例,不然会创建很多连接
                        HBaseDAO.insertByBean(user);
                    }

                });
            }
        });

        // spark streaming rdd 转换为spark sql rdd,统计每个省份的用户注册数量
        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {

            public void call(JavaRDD<String> t) throws Exception {

                // 创建sparkSession对象,sparkContext可以从参数t中获取
                SparkSession sparkSession =
                        SparkSession.builder().config(t.rdd().sparkContext().getConf()).getOrCreate();

                // 将数据转为user对象rdd
                JavaRDD<User> mapRdd = t.map(new Function<String, User>() {

                    public User call(String v1) throws Exception {

                        JSONObject json = new JSONObject(v1);

                        return new User(json.getString("rowKey"), json.getInt("provinceId"), json
                            .getString("province"), json.getString("sex"), json.getLong("time"));
                    }
                });

                // 使用rdd创建dataFrame
                Dataset<Row> dataSet = sparkSession.createDataFrame(mapRdd, User.class);

                // 将数据创建为user_table表
                dataSet.createOrReplaceTempView("user_table");

                // 统计每个省份的用户注册数量
                dataSet.groupBy("province").count().foreach(new ForeachFunction<Row>() {

                    public void call(Row t) throws Exception {
                        System.out.println("spark sql result : " + t);
                    }
                });
            }

        });

        // 开始任务
        javaStreamingContext.start();

        javaStreamingContext.awaitTermination();
    }
}
