package com.dmy.graduation.partitioner;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by DMY on 2018/10/10 15:25
 */
@Service
public class HashPartitionerMock {

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
    private Map<Integer, Integer> partitionSzieMap;

    public HashPartitionerMock() {

    }

    public HashPartitionerMock(int partitionNum, Map<String, Integer> keyCountMap) {
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

    public Map<Integer, Integer> getPartitionSizeMap() {
        return partitionSzieMap;
    }

    public double calculateBalanceRate() {
        assert (partitionNum > 0 && keyCountMap != null);

        partitionKeyMap = new HashMap<>(partitionNum);
        partitionSzieMap = new HashMap<>(partitionNum);

        // 总的键值对数目
        int totalCount = 0;
        for (Map.Entry<String, Integer> entry : keyCountMap.entrySet()) {
            totalCount += entry.getValue();
            // 计算当前key应当放在哪一个partition中
            int partitionId = getPartitionId(entry.getKey(), partitionNum);
            if (!partitionKeyMap.containsKey(partitionId)) {
                partitionKeyMap.put(partitionId, new ArrayList<>());
            }
            partitionKeyMap.get(partitionId).add(entry.getKey());
            partitionSzieMap.put(partitionId, partitionSzieMap.getOrDefault(partitionId, 0) + entry.getValue());
        }

        // 计算不平衡度
        double avgPartitionCount = (double) totalCount / (double) partitionNum;
        double totalDeviation = 0.0;
        for (int i = 0; i < partitionNum; i++) {
            double deviation = avgPartitionCount - partitionSzieMap.getOrDefault(i, 0);
            totalDeviation += Math.pow(deviation, 2);
        }
        double partitionBalanceRate = Math.sqrt(totalDeviation / (double) (partitionNum - 1)) / avgPartitionCount;
        BigDecimal bigDecimal = new BigDecimal(partitionBalanceRate);
        return bigDecimal.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    private int getPartitionId(String key, int partitionNum) {
        int partitionId = key.hashCode() % partitionNum;
        return partitionId < 0 ? partitionId + partitionNum : partitionId;
    }
}
