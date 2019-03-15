package com.dmy.graduation.core;

import com.dmy.graduation.shuffle.SPOROptimization;
import com.dmy.graduation.util.ZipfGenerator;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Int;

import java.util.*;

/**
 * Created by DMY on 2018/11/13 14:49
 */
public class SPOROptimizationTest extends BaseTest {

    @Autowired
    private SPOROptimization sporOptimization;

    @Autowired
    private ZipfGenerator zipfGenerator;

    private static final int KEY_SIZE = 2000;
    private static final int TOTAL_SIZE = 10000 * 10000;
    private static final String PREFIX = "testKey_";

    // 本地120MB/s
    private double localCostPerItem = 0.000001016;

    // 同机架50MB/s
    private double remoteCostPerItem = 0.0000024384;

    private int nodeNum = 10;
    private int partitionNum = 30;

    @Test
    public void testAllocatePartition() {

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
                    int keyCountInPartition = (int) (resKeyCount / Math.pow(2, i + 1));
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

            sporOptimization.nodeNum(nodeNum)
                    .partitionNum(partitionNum)
                    .localCostPerItem(localCostPerItem)
                    .remoteCostPerItem(remoteCostPerItem)
                    .initialLoadMap(initialNodeMap)
                    .originalKeyDistribution(originalKeyDistribution)
                    .keyInPartition(keyInPartition)
                    .initialPartitionDistribution(initialPartitionDistribution);

            System.out.println(sporOptimization.allocateByRoundRobin());
        }

    }

    @Test
    public void test() {


        // 初始计算节点负载
        Map<Integer, Double> initialNodeMap = new HashMap<>();
        Random random = new Random();
        double[] nodeLoadArray = {1.3, 2.6, 1.5, 3.9, 5.1, 2.1, 6.2, 4.3, 3.5, 4.8};
        for (int i = 0; i < 10; i++) {
            initialNodeMap.put(i, nodeLoadArray[i]);
        }

        // 10G数据量大约9千万条数据
        int size = 20000 * 33;
        int totalSize = 0;

        // 计算节点包含各Partition中的数据量
        Map<Integer, List<Integer>> nodeContainsItemMap = new HashMap<>();

        // 每个Partition分布在各计算节点中的数据量
        Map<Integer, List<Integer>> partitionSizeOnNodeMap = new HashMap<>();
        for (int nodeId = 0; nodeId < 10; nodeId++) {
            if (!nodeContainsItemMap.containsKey(nodeId)) {
                nodeContainsItemMap.put(nodeId, new ArrayList<>());
            }
            for (int partitionId = 0; partitionId < 30; partitionId++) {
                int itemSize = random.nextInt(size);
                totalSize += itemSize;
                nodeContainsItemMap.get(nodeId).add(itemSize);
                if (!partitionSizeOnNodeMap.containsKey(partitionId)) {
                    partitionSizeOnNodeMap.put(partitionId, new ArrayList<>());
                }
                partitionSizeOnNodeMap.get(partitionId).add(itemSize);
            }
        }

        Map<Integer, List<Integer>> localityResult = allocatedByLocality(partitionSizeOnNodeMap);
        Map<Integer, List<Integer>> roundRobinResult = allocatedByRoundRobin();

        System.out.println(totalSize);
        Map<Integer, Double> nodeLoadByLocality = calculateNodeLoad(partitionSizeOnNodeMap, localityResult, initialNodeMap);
        System.out.println(nodeLoadByLocality);
        System.out.println("\n=========================================\n");
        Map<Integer, Double> nodeLoadByRoundRobin = calculateNodeLoad(partitionSizeOnNodeMap, roundRobinResult, initialNodeMap);
        System.out.println(nodeLoadByRoundRobin);
    }

    /**
     * 根据最优的数据本地性进行分配
     * @param partitionSizeOnNodeMap
     * @return
     */
    private Map<Integer, List<Integer>> allocatedByLocality(Map<Integer, List<Integer>> partitionSizeOnNodeMap) {
        Map<Integer, List<Integer>> partitionOnNodeMap = new HashMap<>();
        partitionSizeOnNodeMap.forEach((partitionId, itemSizeOnNodeList) -> {
            int max = 0;
            int nodeId = -1;
            for (int i = 0; i < itemSizeOnNodeList.size(); i++) {
                if (itemSizeOnNodeList.get(i) > max) {
                    max = itemSizeOnNodeList.get(i);
                    nodeId = i;
                }
            }
            if (!partitionOnNodeMap.containsKey(nodeId)) {
                partitionOnNodeMap.put(nodeId, new ArrayList<>());
            }
            partitionOnNodeMap.get(nodeId).add(partitionId);
        });
        System.out.println(partitionOnNodeMap);
        return partitionOnNodeMap;
    }

    /**
     * 根据Round Robin算法进行分配
     */
    private Map<Integer, List<Integer>> allocatedByRoundRobin() {
        Map<Integer, List<Integer>> partitionOnNodeMap = new HashMap<>();
        for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
            int nodeId = partitionId % nodeNum;
            if (!partitionOnNodeMap.containsKey(nodeId)) {
                partitionOnNodeMap.put(nodeId, new ArrayList<>());
            }
            partitionOnNodeMap.get(nodeId).add(partitionId);
        }
        return partitionOnNodeMap;
    }

    /**
     * 计算节点负载
     * @param partitionSizeOnNodeMap
     * @param allocationResult
     * @param initialNodeMap
     * @return
     */
    private Map<Integer, Double> calculateNodeLoad(Map<Integer, List<Integer>> partitionSizeOnNodeMap,
                                                   Map<Integer, List<Integer>> allocationResult,
                                                   Map<Integer, Double> initialNodeMap) {
        Map<Integer, Double> nodeLoadMap = new HashMap<>(initialNodeMap);
        allocationResult.forEach((nodeId, partitionList) -> {
            double cost = 0.0;
            for (int partitionId : partitionList) {
                List<Integer> list = partitionSizeOnNodeMap.get(partitionId);
                for (int i = 0; i < list.size(); i++) {
                    if (i == nodeId) {
                        cost += localCostPerItem * list.get(i);
                    } else {
                        cost += remoteCostPerItem * list.get(i);
                    }
                }
            }
            nodeLoadMap.put(nodeId, Double.sum(nodeLoadMap.getOrDefault(nodeId, 0d), cost));
        });
        return nodeLoadMap;
    }
}
