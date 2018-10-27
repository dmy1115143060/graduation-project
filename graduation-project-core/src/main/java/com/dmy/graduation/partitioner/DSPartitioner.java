package com.dmy.graduation.partitioner;

import org.apache.spark.Partitioner;

import java.util.*;

/**
 * Created by DMY on 2018/10/25 17:24
 */
public class DSPartitioner extends Partitioner {

    private int partitionNum;

    private Map<String, Integer> keyCountMap;

    private Map<String, Integer> keyInPartitionMap;

    public DSPartitioner(int partitionNum, Map<String, Integer> keyCountMap) {
        this.partitionNum = partitionNum;
        this.keyCountMap = keyCountMap;
    }

    @Override
    public int getPartition(Object key) {
        String k = (String) key;
        int partitionId = 0;
        if (keyInPartitionMap.containsKey(k)) {
            partitionId =  keyInPartitionMap.get(k);
        } else {
            partitionId = k.hashCode() % numPartitions();
            if (partitionId < 0) {
                partitionId += numPartitions();
            }
        }
        return partitionId;
    }

    @Override
    public int numPartitions() {
        return partitionNum;
    }

    /**
     * 完成数据分区操作
     */
    private void initPartition() {
        keyInPartitionMap = new HashMap<>();

        // 对当前的key按照包含的键值对数目从大到小排序
        List<String> sortedKeyList = new ArrayList<>(keyCountMap.keySet());
        Collections.sort(sortedKeyList, (key1, key2) -> {
            int count1 = keyCountMap.get(key1);
            int count2 = keyCountMap.get(key2);
            return count2 - count1;
        });

        // 对各Partition按照键值对数目构造一个小顶堆
        Map<Integer, Integer> partitionSizeMap = new HashMap<>(partitionNum);
        PriorityQueue<Integer> priorityQueue = new PriorityQueue<>(
                (partitionId1, partitionId2) -> {
                    int partitionPairCount1 = partitionSizeMap.getOrDefault(partitionId1, 0);
                    int partitionPairCount2 = partitionSizeMap.getOrDefault(partitionId2, 0);
                    return partitionPairCount1 - partitionPairCount2;
                });

        // 初始化小顶堆
        for (int i = 0; i < partitionNum; i++) {
            priorityQueue.add(i);
        }

        // 按照List-Scheduling原则来进行数据分区操作
        for (String key : sortedKeyList) {
            int keyCount = keyCountMap.get(key);
            int partitionId = priorityQueue.poll();
            partitionSizeMap.put(partitionId, partitionSizeMap.getOrDefault(partitionId, 0) + keyCount);
            priorityQueue.add(partitionId);
            keyInPartitionMap.put(key, partitionId);
        }
    }
}
