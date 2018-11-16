package com.dmy.graduation.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by DMY on 2018/11/12 15:09
 */
public class SparkUtil {

    /**
     * 统计每个Partition中的数据量
     */
    public List<String> partitionItemsCount(JavaPairRDD<String, String> javaRDD) {
        JavaRDD<String> countRDD = javaRDD.mapPartitionsWithIndex(
                (partitionId, iterator) -> {
                    long count = 0L;
                    while (iterator.hasNext()) {
                        count += 1L;
                        iterator.next();
                    }
                    List<String> list = new ArrayList<>();
                    list.add("partition_" + partitionId + ", " + count);
                    return list.iterator();
                }, true);
        return countRDD.collect();
    }

    /**
     * 统计每个Partition中各个Key的记录数
     */
    public List<Map<Integer, Map<String, Integer>>> partitionKeyTrack(JavaPairRDD<String, String> javaPairRDD) {
        JavaRDD<Map<Integer, Map<String, Integer>>> trackRDD = javaPairRDD.mapPartitionsWithIndex(
                (partitionId, partitionIterator) -> {
                    // 统计每个Partition中的键值Key的分布
                    HashMap<String, Integer> keyCountMap = new HashMap<>();
                    while (partitionIterator.hasNext()) {
                        Tuple2<String, String> tuple2 = partitionIterator.next();
                        keyCountMap.put(tuple2._1, keyCountMap.getOrDefault(tuple2._1, 0) + 1);
                    }
                    List<Map<Integer, Map<String, Integer>>> list = new ArrayList<>();
                    HashMap resultMap = new HashMap();
                    resultMap.put(partitionId, keyCountMap);
                    list.add(resultMap);
                    return list.iterator();
                }, true);
        return trackRDD.collect();
    }

}
