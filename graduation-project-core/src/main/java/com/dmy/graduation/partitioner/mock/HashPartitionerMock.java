package com.dmy.graduation.partitioner.mock;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by DMY on 2018/10/10 15:25
 */
@Service
public class HashPartitionerMock extends DefaultPartitionerMockImpl {

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

    private Map<Integer, List<String>> partitionKeyMap;

    public HashPartitionerMock() {
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

    public Map<Integer, List<String>> getPartitionKeyMap() {
        return partitionKeyMap;
    }

    private void partitionData() {
        assert (partitionNum > 0 && keyCountMap != null);
        partitionSizeMap = new HashMap<>(partitionNum);
        partitionKeyMap = new HashMap<>(partitionNum);
        // 总的键值对数目
        for (Map.Entry<String, Long> entry : keyCountMap.entrySet()) {
            // 计算当前key应当放在哪一个partition中
            int partitionId = getPartitionId(entry.getKey(), partitionNum);
            if(partitionKeyMap.containsKey(partitionId)) {
                partitionKeyMap.put(partitionId, new ArrayList<>());
            }
            partitionKeyMap.get(partitionId).add(entry.getKey());
            partitionSizeMap.put(partitionId, partitionSizeMap.getOrDefault(partitionId, 0L) + entry.getValue());
        }
    }

    private int getPartitionId(String key, int partitionNum) {
        int partitionId = key.hashCode() % partitionNum;
        return partitionId < 0 ? partitionId + partitionNum : partitionId;
    }

    public double calculateTiltRate() {
        partitionData();
        return calculateTiltRate(partitionSizeMap, partitionNum);
    }
}
