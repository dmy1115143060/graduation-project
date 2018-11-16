package com.dmy.graduation.core;


import com.dmy.graduation.spark.SparkUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * Created by DMY on 2018/11/12 15:26
 */
public class SparkUtilTest extends BaseTest {

    @Autowired
    private SparkUtil sparkUtil;

    private JavaSparkContext jsc;

    private JavaPairRDD<String, String> javaPairRDD;

    @Before
    public void init() {
        SparkConf sc = new SparkConf();
        sc.setMaster("local").setAppName("SparkUtilTest");
        jsc = new JavaSparkContext(sc);
        JavaRDD<String> javaRDD = jsc.textFile("G:\\testData", 3);
        javaPairRDD = javaRDD.mapToPair(line -> {
            String[] splits = line.split("\\s+");
            return new Tuple2<>(splits[0], splits[1]);
        });
    }

    @Test
    public void tesPartitionCount() {
        List<String> partitionItemsCountInfo = sparkUtil.partitionItemsCount(javaPairRDD);
        partitionItemsCountInfo.forEach(info -> System.out.println(info));
    }

    @Test
    public void testPartitionKeyTrack() {
        List<Map<Integer, Map<String, Integer>>> partitionKeyCountInfo = sparkUtil.partitionKeyTrack(javaPairRDD);
        partitionKeyCountInfo.forEach(info ->  System.out.println(info));
    }

}
