package com.dmy.graduation.core;

import com.dmy.graduation.shuffle.SCIDOptimization;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.*;

/**
 * Created by DMY on 2018/11/13 14:49
 */
public class SCIDOptimizationTest extends BaseTest {

    @Autowired
    private SCIDOptimization scidOptimization;

    @Before
    public void initial() {
        int nodeNum = 10;
        int partitionNum = 30;

        // 本地120MB/s
        double localCostPerItem = 0.000001016;

        // 同机架50MB/s
        double remoteCostPerItem = 0.0000024384;

        // 初始计算节点负载
        Map<Integer, Double> initialNodeMap = new HashMap<>();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            initialNodeMap.put(i, (double) random.nextInt(10));
        }

        // 随机产生Key集合, 每个Key在Shuffle上游中各Partition的分布以及最终每个Key所属分区
        int parentPartitionNum = 30;
        List<Integer> parentPartitions = new ArrayList<>();
        for (int i = 0; i < parentPartitionNum; i++) {
            parentPartitions.add(i);
        }
        Map<Integer, Map<String, Integer>> originalKeyDistribution = new HashMap<>();
        Map<String, Integer> keyInPartition = new HashMap<>();

        int partitionIndex = 0;
        int totalItemSize = 0;
        for (int i = 0; i < 2000; i++) {
            String key = "testKey" + "_" + i;
            Collections.shuffle(parentPartitions);
            for (int j = 0; j < 10; j++) {
                int partitionId = parentPartitions.get(j);
                if (!originalKeyDistribution.containsKey(partitionId)) {
                    originalKeyDistribution.put(partitionId, new HashMap<>());
                }
                int keyItemSize = random.nextInt(3000 * (j + 1));
                totalItemSize += keyItemSize;
                originalKeyDistribution.get(partitionId).put(key, keyItemSize);
            }
            keyInPartition.put(key, partitionIndex);
            partitionIndex++;
            if (partitionIndex == partitionNum) {
                partitionIndex = 0;
            }
        }

        System.out.println("键值对数目：" + totalItemSize);

        // 让初始Partition集合分布在各计算节点上
        Map<Integer, List<Integer>> initialPartitionDistribution = new HashMap<>();
        int nodeId = 0;
        for (int partitionId = 0; partitionId < parentPartitionNum; partitionId++) {
            if (!initialPartitionDistribution.containsKey(nodeId)) {
                initialPartitionDistribution.put(nodeId, new ArrayList<>());
            }
            initialPartitionDistribution.get(nodeId).add(partitionId);
            nodeId = (nodeId + 1) % nodeNum;
        }

        scidOptimization.nodeNum(nodeNum)
                .partitionNum(partitionNum)
                .localCostPerItem(localCostPerItem)
                .remoteCostPerItem(remoteCostPerItem)
                .initialLoadMap(initialNodeMap)
                .originalKeyDistribution(originalKeyDistribution)
                .keyInPartition(keyInPartition)
                .initialPartitionDistribution(initialPartitionDistribution);
    }

    @Test
    public void testAllocatePartition() {
        scidOptimization.allocatePartition();
    }
}
