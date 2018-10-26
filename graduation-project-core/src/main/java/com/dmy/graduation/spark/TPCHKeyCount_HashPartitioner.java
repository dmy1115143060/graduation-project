package com.dmy.graduation.spark;

import org.apache.spark.HashPartitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by DMY on 2018/10/24 20:58
 */
public class TPCHKeyCount_HashPartitioner {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("请输入分区数、APP名称以及数据来源！");
            return;
        }
        int partitionNum = Integer.parseInt(args[0]);
        String appName = args[1];
        String filePath = args[2];
        SparkConf sc = new SparkConf();
        sc.setAppName(appName);
        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> inputRDD = jsc.textFile(filePath);
        JavaPairRDD<String, String> pairRDD = inputRDD.mapToPair(line -> {
            String[] splits = line.split("\\|");
            if (splits.length > 2) {
                return new Tuple2<>(splits[2], line);
            }
            return new Tuple2<>(" ", line);
        }).filter(tuple2 -> !tuple2._1.equals(" "));

        JavaPairRDD<String, Iterable<String>> groupRDD = pairRDD.groupByKey(new HashPartitioner(partitionNum));

        JavaPairRDD<String, Long> keyCountRDD = groupRDD.mapToPair(tuple2 -> {
            long count = 0L;
            Iterator<String> iterator = tuple2._2.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return new Tuple2<>(tuple2._1, count);
        });

        Map<String, Long> keyCountMap = keyCountRDD.collectAsMap();
        try {
            String fileName = appName + ".txt";
            BufferedWriter writer = new BufferedWriter(new FileWriter("/home/jjin/dumingyang/result/" + fileName));
            for (Map.Entry<String, Long> entry : keyCountMap.entrySet()) {
                writer.write(entry.getKey() + ":" + entry.getValue());
                writer.newLine();
                writer.flush();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
