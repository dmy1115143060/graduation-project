package com.dmy.graduation.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Map;

/**
 * Created by DMY on 2018/12/20 9:30
 *
 * 获取键值对信息对应的时间：reduceByKey算子
 */
public class GetFullDataTime {

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("请输入分区数、APP名称以及数据来源！");
            return;
        }

        System.out.println("============================================\n\n");
        System.out.println("开始执行程序!");
        System.out.println("============================================\n\n");

        int partitionNum = Integer.parseInt(args[0]);
        String appName = args[1];
        String filePath = args[2];
        SparkConf sc = new SparkConf();
        sc.setAppName(appName);
        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> inputRDD = jsc.textFile(filePath);
        JavaPairRDD<String, Long> pairRDD = inputRDD.mapToPair(line -> {
            String[] splits = line.split("\\|");
            if (splits.length > 2) {
                return new Tuple2<>(splits[2], 1L);
            }
            return new Tuple2<>(" ", 1L);
        }).filter(tuple2 -> !tuple2._1.equals(" ")).cache();

        System.out.println(pairRDD.count());

        long time1 = System.currentTimeMillis();
        JavaPairRDD<String, Long> keyCountRDD = pairRDD.reduceByKey((count1, count2) -> count1 + count2, partitionNum);
        Map<String, Long> keyCountMap = keyCountRDD.collectAsMap();
        long time2 = System.currentTimeMillis();
        System.out.println("============================================\n\n");
        System.out.println("成功获取数据信息，获取数据时间为：" + (time2 - time1) / 1000 + "s");
        System.out.println("============================================\n\n");
    }
}
