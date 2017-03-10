package com.youanmi.bi.kafkaSpark.demo;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;


/**
 * kafka生产者demo,模拟发送用户注册信息到kafka
 * 
 * @author tanguojun
 *
 */
public class KafkaProducerDemo extends Thread {

    public static final String TOPIC = "user_register";

    public static final String[] PROVINCES = new String[] {"北京市", "天津市", "上海市", "重庆市", "河北省", "山西省", "辽宁省",
                                                           "吉林省", "黑龙江省", "江苏省", "浙江省", "安徽省", "福建省", "江西省",
                                                           "山东省", "河南省", "湖北省", "湖南省", "广东省", "海南省", "四川省",
                                                           "贵州省", "云南省", "陕西省", "甘肃省", "青海省", "台湾省",
                                                           "内蒙古自治区", "广西壮族自治区", "西藏自治区", "宁夏回族自治区",
                                                           "新疆维吾尔自治区", "香港特别行政区", "澳门特别行政区"};

    public static final String[] SEXS = new String[] {"男", "女"};


    public static void main(String[] args) {
        // 运行kafka,模拟发送注册消息
        new KafkaProducerDemo().run();
    }


    @Override
    public void run() {
        Properties props = new Properties();
        // 设置kafka服务器的地址
        props.put("bootstrap.servers", "192.168.1.6:9092");
        // The "all" setting we have specified will result in blocking on the
        // full commit of the record, the slowest but most durable setting.
        // “所有”设置将导致记录的完整提交阻塞","最慢的","但最持久的设置。
        props.put("acks", "all");
        // 如果请求失败","生产者也会自动重试","即使设置成０ the producer can automatically retry.
        props.put("retries", 0);

        // The producer maintains buffers of unsent records for each partition.
        props.put("batch.size", 16384);
        // 默认立即发送","这里这是延时毫秒数
        props.put("linger.ms", 1);
        // 生产者缓冲大小","当缓冲区耗尽后","额外的发送调用将被阻塞。时间超过max.block.ms将抛出TimeoutException
        props.put("buffer.memory", 33554432);
        // The key.serializer and value.serializer instruct how to turn the key
        // and value objects the user provides with their ProducerRecord into
        // bytes.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建kafka的生产者类,生产者的主要方法
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        System.out.println("kafka producer send start.");

        Random random = new Random();
        JSONObject json = new JSONObject();

        // 随机生成消息
        for (int i = 1; i <= 1; i++) {
            int pronvinceId = random.nextInt(PROVINCES.length);
            json.put("provinceId", pronvinceId);
            json.put("province", PROVINCES[pronvinceId]);
            json.put("sex", SEXS[random.nextInt(2)]);
            json.put("age", random.nextInt(80));
            json.put("time", System.currentTimeMillis());
            // 发送消息到kafka
            Future<RecordMetadata> result =
                    producer.send(new ProducerRecord<String, String>(TOPIC, json.toString()));
            try {
                // 打印发送结果
                System.out.println(result.get());
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            if (i % 10000 == 0) {
                try {
                    Thread.sleep(10000L);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("kafka producer send end.");

        producer.close();

        System.out.println("kafka producer close");

    }

}
