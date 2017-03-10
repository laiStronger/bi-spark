package com.youanmi.bi.spark.demo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;


/**
 * rdd分区例子
 * 
 * @author tanguojun
 *
 */
public class RDDPartitionDemo {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local[2]").setAppName("RDDPartitionDemo");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 原数据
        List<Integer> data = Arrays.asList(1, 2, 2, 3, 3, 4, 4);

        // 分3个partition处理
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(data, 3);

        // 获得分区ID
        JavaRDD<String> partitionRDD =
                javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {

                    public Iterator<String> call(Integer v1, Iterator<Integer> v2) throws Exception {
                        LinkedList<String> linkedList = new LinkedList<String>();
                        while (v2.hasNext()) {
                            linkedList.add("分区" + v1 + "数据为:" + v2.next());
                        }
                        return linkedList.iterator();
                    }
                }, false);

        // 打印出即可看出数据分区
        System.out.println(partitionRDD.collect());

        // 按分区拿到数据
        javaRDD.foreachPartition(new VoidFunction<Iterator<Integer>>() {
            public void call(Iterator<Integer> integerIterator) throws Exception {
                System.out.print("此分区有数据:");
                while (integerIterator.hasNext()) {
                    System.out.print("[" + integerIterator.next() + "]");
                }
                System.out.println();
            }
        });

    }

}
