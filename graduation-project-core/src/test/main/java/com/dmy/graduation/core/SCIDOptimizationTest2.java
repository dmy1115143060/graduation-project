package com.dmy.graduation.core;

import com.dmy.graduation.shuffle.SCIDOptimization;
import com.dmy.graduation.util.ZipfGenerator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

/**
 * Created by DMY on 2018/11/13 14:49
 */
public class SCIDOptimizationTest2 extends BaseTest {

    @Autowired
    private SCIDOptimization scidOptimization;

    @Autowired
    private ZipfGenerator zipfGenerator;

    private static final int KEY_SIZE = 2000;
    private static final int TOTAL_SIZE = 10000 * 15000;
    private static final String PREFIX = "testKey_";

    @Test
    public void testAllocatePartition() {

        int nodeNum = 10;
        int partitionNum = 30;

        // 本地120MB/s
        double localCostPerItem = 0.000001016;

        // 同机架50MB/s
        double remoteCostPerItem = 0.0000024384;

        // 初始计算节点负载
        Map<Integer, Double> initialNodeMap = new HashMap<>();
        Random random = new Random();
        double[] nodeLoadArray = {1.3, 2.6, 1.5, 3.9, 5.1, 2.1, 6.2, 4.3, 3.5, 4.8};
        for (int i = 0; i < 10; i++) {
            initialNodeMap.put(i, nodeLoadArray[i]);
        }

        int parentPartitionNum = 30;
        List<Integer> parentPartitions = new ArrayList<>();
        for (int i = 0; i < parentPartitionNum; i++) {
            parentPartitions.add(i);
        }

        //double[] skewArray = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
        double[] skewArray = {0.0};
        for (double skew : skewArray) {

            System.out.println("\n\n" + "=========================skew：" + skew + "=============================");

            Map<Integer, Map<String, Integer>> originalKeyDistribution = new HashMap<>();
            Map<String, Integer> keyInPartition = new HashMap<>();

            int partitionIndex = 0;

            // 先计算出所有Key对应的键值对个数
            Map<String, Integer> keyCountMap = new HashMap<>(KEY_SIZE);
            zipfGenerator.setSize(KEY_SIZE);
            zipfGenerator.setSkew(skew);
            for (int i = 0; i < TOTAL_SIZE; i++) {
                String key = PREFIX + zipfGenerator.next();
                keyCountMap.put(key, keyCountMap.getOrDefault(key, 0) + 1);
            }
            //System.out.println(keyCountMap);

            // 将每个Key包含的键值对随机分布在各个Partition中
            Map<Integer, Integer> partitionSizeMap = new HashMap<>();
            for (Map.Entry<String, Integer> entry : keyCountMap.entrySet()) {
                Collections.shuffle(parentPartitions);
                String key = entry.getKey();
                int resKeyCount = entry.getValue();
                for (int i = 0; i < 10; i++) {
                    int keyCountInPartition = (int)(resKeyCount / Math.pow(2, i + 1));
                    if (i == 9) {
                        keyCountInPartition = resKeyCount;
                    }
                    int partitionId = parentPartitions.get(i);
                    if (!originalKeyDistribution.containsKey(partitionId)) {
                        originalKeyDistribution.put(partitionId, new HashMap<>());
                    }
                    originalKeyDistribution.get(partitionId).put(key, keyCountInPartition);
                    resKeyCount -= keyCountInPartition;
                    if (resKeyCount == 0) {
                        break;
                    }
                }
                keyInPartition.put(key, partitionIndex);
                partitionSizeMap.put(partitionIndex, partitionSizeMap.getOrDefault(partitionIndex, 0) + keyCountMap.get(key));
                partitionIndex++;
                if (partitionIndex == partitionNum) {
                    partitionIndex = 0;
                }
            }

//            System.out.println("\n\n各Partition中包含的数据量为：");
//            System.out.println(partitionSizeMap);
//            System.out.println("\n\n");

            // 让初始Partition集合随机分布在各计算节点上
            Map<Integer, List<Integer>> initialPartitionDistribution = new HashMap<>();
            for (int partitionId = 0; partitionId < parentPartitionNum; partitionId++) {
                int nodeId = random.nextInt(nodeNum);
                if (!initialPartitionDistribution.containsKey(nodeId)) {
                    initialPartitionDistribution.put(nodeId, new ArrayList<>());
                }
                initialPartitionDistribution.get(nodeId).add(partitionId);
            }

            scidOptimization.nodeNum(nodeNum)
                    .partitionNum(partitionNum)
                    .localCostPerItem(localCostPerItem)
                    .remoteCostPerItem(remoteCostPerItem)
                    .initialLoadMap(initialNodeMap)
                    .originalKeyDistribution(originalKeyDistribution)
                    .keyInPartition(keyInPartition)
                    .initialPartitionDistribution(initialPartitionDistribution);

            scidOptimization.allocatePartition();
        }

    }
}
