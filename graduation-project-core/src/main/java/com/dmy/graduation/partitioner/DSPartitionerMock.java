package com.dmy.graduation.partitioner;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;

/**
 * Created by DMY on 2018/10/10 15:26
 */
@Service
public class DSPartitionerMock {

    /**
     * 分区个数
     */
    private int partitionNum;

    /**
     * key: 关键字   value: 关键字出现次数
     */
    private Map<String, Integer> keyCountMap;

    /**
     * key: partitionId  value: 当前partition包含的key集合
     */
    private Map<Integer, List<String>> partitionKeyMap;

    /**
     * key: partitionId  value: 当前partition包含的键值对个数
     */
    private Map<Integer, Integer> partitionSizeMap;

    public DSPartitionerMock() {

    }

    public DSPartitionerMock(int partitionNum, Map<String, Integer> keyCountMap) {
        this.partitionNum = partitionNum;
        this.keyCountMap = keyCountMap;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public void setKeyCountMap(Map<String, Integer> keyCountMap) {
        this.keyCountMap = keyCountMap;
    }

    public Map<Integer, List<String>> getPartitionKeyMap() {
        return partitionKeyMap;
    }

    public Map<Integer, Integer> getPartitionSzieMap() {
        return partitionSizeMap;
    }

    public double calculateBalanceRate() {

        assert (partitionNum > 0 && keyCountMap != null);

        partitionKeyMap = new HashMap<>(partitionNum);
        partitionSizeMap = new HashMap<>(partitionNum);
        List<String> sortedkeyList = new ArrayList<>(keyCountMap.keySet());

        // 对当前的key按照包含的键值对数目从大到小排序
        Collections.sort(sortedkeyList, (key1, key2) -> {
            int count1 = keyCountMap.get(key1);
            int count2 = keyCountMap.get(key2);
            return count2 - count1;
        });

        // 对各Reducer构造一个小顶堆
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

        // 每次将包含最多键值对的key分配给小顶堆堆顶的partition
        int totalCount = 0;
        for (String key : sortedkeyList) {
            int keyCount = keyCountMap.get(key);
            totalCount += keyCount;
            int reducerID = priorityQueue.poll();
            partitionSizeMap.put(reducerID, partitionSizeMap.getOrDefault(reducerID, 0) + keyCount);
            priorityQueue.add(reducerID);
            if (!partitionKeyMap.containsKey(reducerID)) {
                partitionKeyMap.put(reducerID, new ArrayList<>());
            }
            partitionKeyMap.get(reducerID).add(key);
        }

        // 计算partition不均衡度
        double avgPartitionCount = (double) totalCount / (double) partitionNum;
        double totalDeviation = 0;
        for (int i = 0; i < partitionNum; i++) {
            double deviation = avgPartitionCount - partitionSizeMap.getOrDefault(i, 0);
            totalDeviation += Math.pow(deviation, 2);
        }
        double reducerBalanceRate = (Math.sqrt(totalDeviation / (double) (partitionNum - 1))) / avgPartitionCount;
        BigDecimal bd = new BigDecimal(reducerBalanceRate);
        return bd.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
