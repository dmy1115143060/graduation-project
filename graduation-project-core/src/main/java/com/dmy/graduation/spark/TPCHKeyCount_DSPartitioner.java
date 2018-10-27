package com.dmy.graduation.spark;

import com.dmy.graduation.partitioner.DSPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by DMY on 2018/10/24 20:58
 */
public class TPCHKeyCount_DSPartitioner {

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("请输入分区数、APP名称、数据来源以及各个Key出现的频率数据！");
            return;
        }

        System.out.println("============================================\n\n");
        System.out.println("开始执行程序!");
        System.out.println("============================================\n\n");

        int partitionNum = Integer.parseInt(args[0]);
        String appName = args[1];
        String originalFilePath = args[2];
        String keyCountFilePath = args[3];
        SparkConf sc = new SparkConf();
        sc.setAppName(appName);
        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> inputRDD = jsc.textFile(originalFilePath);
        JavaPairRDD<String, String> pairRDD = inputRDD.mapToPair(line -> {
            String[] splits = line.split("\\|");
            if (splits.length > 2) {
                return new Tuple2<>(splits[2], line);
            }
            return new Tuple2<>(" ", line);
        }).filter(tuple2 -> !tuple2._1.equals(" "));

        Map<String, Integer> keyCountMap = loadKeyCount(keyCountFilePath);
        DSPartitioner dsPartitioner = new DSPartitioner(partitionNum, keyCountMap);
        JavaPairRDD<String, Iterable<String>> groupRDD = pairRDD.groupByKey(dsPartitioner);

        JavaPairRDD<String, Long> keyCountRDD = groupRDD.mapToPair(tuple2 -> {
            long count = 0L;
            Iterator<String> iterator = tuple2._2.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return new Tuple2<>(tuple2._1, count);
        });

        System.out.println("============================================\n\n");
        System.out.println(keyCountRDD.count());
        System.out.println("成功执行程序!");
        System.out.println("============================================\n\n");
    }

    /**
     * 加载文件获得每个key的键值对分配
     */
    private static Map<String, Integer> loadKeyCount(String filePath) {
        BufferedReader reader = null;
        Map<String, Integer> keyCountMap = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            keyCountMap = new HashMap<>();
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] splits = line.split(":");
                if (splits.length == 2) {
                    String key = splits[0];
                    int count = Integer.parseInt(splits[1]);
                    keyCountMap.put(key, count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return keyCountMap;
    }
}
