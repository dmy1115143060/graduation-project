package com.dmy.graduation.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * Created by DMY on 2018/12/20 9:30
 *
 * 获取键值对信息对应的时间：数据抽样获取相关信息
 */
public class GetSampleDataTime {

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("请输入APP名称、数据来源以及抽样数据量！");
            return;
        }

        System.out.println("============================================\n\n");
        System.out.println("开始执行程序!");
        System.out.println("============================================\n\n");

        String appName = args[0];
        String filePath = args[1];
        int sampleSize = Integer.parseInt(args[2]);

        SparkConf sc = new SparkConf();
        sc.setAppName(appName);
        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaRDD<String> inputRDD = jsc.textFile(filePath).cache();
        System.out.println(inputRDD.count());

        long time1 = System.currentTimeMillis();
        int sampleSizePerPartition = sampleSize / inputRDD.partitions().size();
        Broadcast<Integer> sampleBroadcast = jsc.broadcast(sampleSizePerPartition);
        // 遍历每个分区，对每个分区中的数据使用水塘抽样算法进行抽样
        JavaPairRDD<Integer, Map<String, Integer>> sampledRDD = inputRDD.mapPartitionsToPair(iter -> {
            int capacity = sampleBroadcast.getValue();
            List<String> pool = new ArrayList<>(capacity);
            List<Tuple2<Integer, Map<String, Integer>>> result = new ArrayList<>();
            int curCapacity = 0;
            Random random = new Random();
            while (iter.hasNext()) {
                String line = iter.next();
                String[] splits = line.split("\\|");
                if (splits.length > 2) {
                    if (curCapacity < capacity) {
                        pool.add(splits[2]);
                    } else {
                        int index = random.nextInt(capacity);
                        pool.remove(index);
                        pool.add(splits[2]);
                    }
                    curCapacity++;
                }
            }
            Map<String, Integer> keyCountMap = new HashMap<>(capacity);
            for (String key : pool) {
                keyCountMap.put(key, keyCountMap.getOrDefault(key, 0) + 1);
            }
            result.add(new Tuple2<>(curCapacity, keyCountMap));
            return result.iterator();
        });

        List<Tuple2<Integer, Map<String, Integer>>> sampledList = sampledRDD.collect();
        int totalSize = 0;
        Map<String, Integer> sampledKeyCountMap = new HashMap<>();
        for (Tuple2<Integer, Map<String, Integer>> tuple2 : sampledList) {
            totalSize += tuple2._1;
            Map<String, Integer> sampledInfoMap = tuple2._2;
            for (Map.Entry<String, Integer> entry : sampledInfoMap.entrySet()) {
                sampledKeyCountMap.put(entry.getKey(), sampledKeyCountMap.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
        }
        long time2 = System.currentTimeMillis();
        System.out.println("============================================\n\n");
        System.out.println("totalSize: " + totalSize);
        System.out.println("成功获取数据信息，获取数据时间为：" + (time2 - time1) / 1000 + "s");
        System.out.println("============================================\n\n");
    }
}
