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

    public DSPartitionerMock(int partitionNum, Map<String, Integer> keyCountMap) {
        this.partitionNum = partitionNum;
        this.keyCountMap = keyCountMap;
    }

    public double calculateBalanceRate() {

        assert (partitionNum > 0 && keyCountMap != null);

        Map<Integer, List<String>> partitionKeyMap = new HashMap<>();
        Map<Integer, Integer> partitionPairCountMap = new HashMap<>();
        List<String> keyList = new ArrayList<>(keyCountMap.keySet());

        // 对当前的key按照包含的键值对数目从大到小排序
        Collections.sort(keyList, (key1, key2) -> {
            int count1 = keyCountMap.get(key1);
            int count2 = keyCountMap.get(key2);
            return count2 - count1;
        });

        // 对各Reducer构造一个小顶堆
        PriorityQueue<Integer> priorityQueue = new PriorityQueue<>(
                (partitionId1, partitionId2) -> {
                    int partitionPairCount1 = partitionPairCountMap.getOrDefault(partitionId1, 0);
                    int partitionPairCount2 = partitionPairCountMap.getOrDefault(partitionId2, 0);
                    return partitionPairCount1 - partitionPairCount2;
                });

        // 初始化小顶堆
        for (int i = 0; i < partitionNum; i++)
            priorityQueue.add(i);

        // 每次将包含最多键值对的key分配给小顶堆堆顶的partition
        int totalCount = 0;
        for (String key : keyList) {
            int keyCount = keyCountMap.get(key);
            totalCount += keyCount;
            int reducerID = priorityQueue.poll();
            partitionPairCountMap.put(reducerID, partitionPairCountMap.getOrDefault(reducerID, 0) + keyCount);
            priorityQueue.add(reducerID);
            if (!partitionKeyMap.containsKey(reducerID)) {
                partitionKeyMap.put(reducerID, new ArrayList<>());
            }
            partitionKeyMap.get(reducerID).add(key);
        }

        // 计算partition不均衡度
        double averagePairCount = (double) totalCount / (double) partitionNum;
        double totalDeviation = 0;
        for (int reducerID : partitionPairCountMap.keySet()) {
            //System.out.println("(" + reducerID + "," + keyCountInReducers.get(reducerID) + ")");
            int reducerKeyCount = partitionPairCountMap.get(reducerID);
            double deviation = averagePairCount - reducerKeyCount;
            totalDeviation += Math.pow(deviation, 2);
        }
        double reducerBalanceRate = (Math.sqrt(totalDeviation / (double) (partitionNum - 1))) / averagePairCount;
        BigDecimal bd = new BigDecimal(reducerBalanceRate);
        return bd.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
