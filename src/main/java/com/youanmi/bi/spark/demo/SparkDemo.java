package com.youanmi.bi.spark.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


/**
 * 简单的spark实例,收集日志里的单词个数
 * 
 * @author tanguojun
 *
 */
public class SparkDemo {

    private static final Logger log = Logger.getLogger(SparkDemo.class);


    public static void main(String[] args) {

        // 创建spark环境配置,设置master为local[2]的意思为本地开发运行,[2]代表cpu数量,appName为任务名,在web界面上可以看到此名称
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("test1");
        // SparkConf sparkConf = new
        // SparkConf().setMaster(args[0]).setAppName("test2");

        // 创建sparkContext
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 读取smp.log文件
        JavaRDD<String> rdd1 = javaSparkContext.textFile("E:/smp.log");

        // 过滤
        JavaRDD<String> rdd2 = rdd1.filter(new Function<String, Boolean>() {

            public Boolean call(String t) throws Exception {
                // 不收集error和warn的日志收集
                if (t.contains("[ERROR") || t.contains("[WARN")) {
                    return false;
                }
                // 不收集exception和错误堆栈的日志
                else if (t.indexOf("Exception") >= 0 || t.indexOf("at ") >= 0) {
                    return false;
                }
                // 否则返回true
                return true;
            }
        });

        // 切分所有的单词
        JavaRDD<String> rdd3 = rdd2.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String t) throws Exception {
                List<String> words = new ArrayList<String>();
                // 把一行中的所有其他符号替换为.符号
                String t1 =
                        t.replaceAll(" ", ".").replaceAll("　", ".").replaceAll(",", ".").replaceAll(":", ".")
                            .replaceAll("/", ".").replaceAll("\\[", ".").replaceAll("\\]", ".")
                            .replaceAll("\\(", ".").replaceAll("\\)", ".").replaceAll("=", ".")
                            .replaceAll("\\|", ".").replaceAll("#", ".");
                // 以.号切分
                words.addAll(Arrays.asList(t1.split("\\.")));
                return words.iterator();
            }
        });

        // 把单词统计
        JavaPairRDD<String, Integer> rdd4 = rdd3.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<String, Integer>(t, 1);
            }
        });

        // 根据单词reduce,然后计算个数
        JavaPairRDD<String, Integer> rdd5 = rdd4.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<String, Integer>> list = rdd5.collect();

        // 打印单词出现的数量
        for (Tuple2<String, Integer> t : list) {
            System.out.println(t);
        }

        javaSparkContext.stop();
        javaSparkContext.close();
    }
}
