package com.dmy.graduation.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DMY on 2018/11/1 10:10
 */
public class WordCount {
    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("请输入源文件位置已经结果输出位置！");
            return;
        }

        System.out.println("============================================\n\n");
        System.out.println("开始执行程序!");
        System.out.println("\n\n============================================");

        String originalFilePath = args[0];
        String resultPath = args[1];
        SparkConf sc = new SparkConf();
        sc.setAppName("spark_wordcount");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        // 加载数据
        JavaRDD<String> inputRDD = jsc.textFile(originalFilePath);

        // 将每行数据分割成一个个键值对的形式，每行数据包含若干个单词，每个单词使用空格分隔开
        JavaPairRDD<String, Long> pairRDD = inputRDD.flatMapToPair(line -> {
            String[] splits = line.split("\\s+");
            List<Tuple2<String, Long>> tuple2List = new ArrayList<>();
            for (String word : splits) {
                tuple2List.add(new Tuple2<>(word, 1L));
            }
            return tuple2List.iterator();
        });

        JavaPairRDD<String, Long> keyCountPairRDD = pairRDD.reduceByKey((count1, count2) -> count1 + count2).repartition(1);
        keyCountPairRDD.saveAsTextFile(resultPath);

        System.out.println("============================================\n\n");
        System.out.println("成功执行程序!");
        System.out.println("\n\n============================================");
    }
}
