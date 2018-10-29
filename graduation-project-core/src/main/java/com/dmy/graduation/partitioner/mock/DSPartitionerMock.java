package com.dmy.graduation.partitioner.mock;

import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Created by DMY on 2018/10/10 15:26
 */
@Service
public class DSPartitionerMock extends DefaultPartitionerMockImpl {

    /**
     * 分区个数
     */
    private int partitionNum;

    /**
     * key: 关键字   value: 关键字出现次数
     */
    private Map<String, Long> keyCountMap;

    /**
     * key: partitionId  value: 当前partition包含的键值对个数
     */
    private Map<Integer, Long> partitionSizeMap;

    public DSPartitionerMock() {
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public void setKeyCountMap(Map<String, Long> keyCountMap) {
        this.keyCountMap = keyCountMap;
    }

    public Map<Integer, Long> getPartitionSizeMap() {
        return partitionSizeMap;
    }

    private void partitionData() {
        assert (partitionNum > 0 && keyCountMap != null);

        partitionSizeMap = new HashMap<>(partitionNum);
        List<String> sortedKeyList = new ArrayList<>(keyCountMap.keySet());

        // 对当前的key按照包含的键值对数目从大到小排序
        Collections.sort(sortedKeyList, (key1, key2) -> {
            long count1 = keyCountMap.get(key1);
            long count2 = keyCountMap.get(key2);
            return (int) (count2 - count1);
        });

        // 对各Partition构造一个小顶堆
        PriorityQueue<Integer> priorityQueue = new PriorityQueue<>(
                (partitionId1, partitionId2) -> {
                    long partitionPairCount1 = partitionSizeMap.getOrDefault(partitionId1, 0L);
                    long partitionPairCount2 = partitionSizeMap.getOrDefault(partitionId2, 0L);
                    return (int) (partitionPairCount1 - partitionPairCount2);
                });

        // 初始化小顶堆
        for (int i = 0; i < partitionNum; i++) {
            priorityQueue.add(i);
        }

        // 每次将包含最多键值对的key分配给小顶堆堆顶的partition
        for (String key : sortedKeyList) {
            long keyCount = keyCountMap.get(key);
            int partitionId = priorityQueue.poll();
            partitionSizeMap.put(partitionId, partitionSizeMap.getOrDefault(partitionId, 0L) + keyCount);
            priorityQueue.add(partitionId);
        }
    }

    public double calculateTiltRate() {
        partitionData();
        return super.calculateTiltRate(partitionSizeMap, partitionNum);
    }
}
