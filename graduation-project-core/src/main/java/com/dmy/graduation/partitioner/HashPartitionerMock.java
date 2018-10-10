package com.dmy.graduation.partitioner;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by DMY on 2018/10/10 15:25
 */
public class HashPartitionerMock {

    /**
     * 分区个数
     */
    private int partitionNum;

    /**
     * key: 关键字   value: 关键字出现次数
     */
    private Map<String, Integer> keyCountMap;

    public HashPartitionerMock(int partitionNum, Map<String, Integer> keyCountMap) {
        this.partitionNum = partitionNum;
        this.keyCountMap = keyCountMap;
    }

    public double calculateBalanceRate() {
        assert (partitionNum > 0 && keyCountMap != null);

        // 分别记录一个partition中包含哪些key以及每个partition当前包含的键值对个数
        Map<Integer, List<String>> partitionKeyMap = new HashMap<>();
        Map<Integer, Integer> partitionPairCountMap = new HashMap<>();

        // 总的键值对数目
        int totalCount = 0;
        for (Map.Entry<String, Integer> entry : keyCountMap.entrySet()) {
            totalCount += entry.getValue();
            // 计算当前key应当放在哪一个partition中
            int partitionId = entry.getKey().hashCode() % partitionNum;
            if (!partitionKeyMap.containsKey(partitionId)) {
                partitionKeyMap.put(partitionId, new ArrayList<>());
            }
            partitionKeyMap.get(partitionId).add(entry.getKey());
            partitionPairCountMap.put(partitionId, partitionPairCountMap.getOrDefault(partitionId, 0) + entry.getValue());
        }

        // 计算不平衡度
        double avgPartitionCount = (double) totalCount / (double) partitionNum;
        double totalDeviation = 0.0;
        for (Map.Entry<Integer, Integer> entry : partitionPairCountMap.entrySet()) {
            double deviation = avgPartitionCount - entry.getValue();
            totalDeviation += Math.pow(deviation, 2);
        }
        double partitionBalanceRate = Math.sqrt(totalDeviation / (double) (partitionNum - 1)) / avgPartitionCount;
        BigDecimal bigDecimal = new BigDecimal(partitionBalanceRate);
        return bigDecimal.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
