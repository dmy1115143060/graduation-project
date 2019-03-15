package com.dmy.graduation.shuffle;

import scala.Int;

import java.util.*;

/**
 * Created by DMY on 2019/3/12 10:01
 * 基于Round Robin算法实现的Partition放置策略
 */
public class SPOROptimization {

    /**
     * 节点数目以及分区数目
     */
    private int nodeNum;
    private int partitionNum;

    /**
     * 本地、远程读取一条键值对所需要的开销
     */
    private double localCostPerItem;
    private double remoteCostPerItem;

    /**
     * 计算节点的初始负载。(节点标识，初始负载值)
     */
    private Map<Integer, Double> initialLoadMap;

    /**
     * 每个Key所在Shuffle生成的Partition映射(键值Key, partitionId)
     */
    private Map<String, Integer> keyInPartition;

    /**
     * Shuffle上游各Partition中的键值Key分布。(partitionId, (键值Key, 键值Key在该Partition中的记录数))
     */
    private Map<Integer, Map<String, Integer>> originalKeyDistribution;

    /**
     * 初始的Partition分布。(计算节点标识, 初始分布在该计算节点上的Partition集合)
     */
    private Map<Integer, List<Integer>> initialPartitionDistribution;

    public SPOROptimization() {

    }

    public SPOROptimization nodeNum(int nodeNum) {
        this.nodeNum = nodeNum;
        return this;
    }

    public SPOROptimization partitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
        return this;
    }

    public SPOROptimization localCostPerItem(double localCostPerItem) {
        this.localCostPerItem = localCostPerItem;
        return this;
    }

    public SPOROptimization remoteCostPerItem(double remoteCostPerItem) {
        this.remoteCostPerItem = remoteCostPerItem;
        return this;
    }

    public SPOROptimization initialLoadMap(Map<Integer, Double> initialLoadMap) {
        this.initialLoadMap = initialLoadMap;
        return this;
    }

    public SPOROptimization keyInPartition(Map<String, Integer> keyInPartition) {
        this.keyInPartition = keyInPartition;
        return this;
    }

    public SPOROptimization originalKeyDistribution(Map<Integer, Map<String, Integer>> originalKeyDistribution) {
        this.originalKeyDistribution = originalKeyDistribution;
        return this;
    }

    public SPOROptimization initialPartitionDistribution(Map<Integer, List<Integer>> initialPartitionDistribution) {
        this.initialPartitionDistribution = initialPartitionDistribution;
        return this;
    }

    public Map<Integer, List<Integer>> allocateByRoundRobin() {
        // 将计算节点进行打散
        List<Integer> nodeList = new ArrayList<>(nodeNum);
        for (int i = 0; i < nodeNum; i++) {
            nodeList.add(i);
        }

        // 循环轮转的方式将Partition分配给所有的计算节点
        Map<Integer, List<Integer>> allocatedMap = new HashMap<>();
        for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
            int index = partitionId % nodeNum;
            int nodeId = nodeList.get(index);
            if (!allocatedMap.containsKey(nodeId)) {
                allocatedMap.put(nodeId, new ArrayList<>());
            }
            allocatedMap.get(nodeId).add(partitionId);
        }

        // 计算此时所有节点的负载
        calculateNodeLoad(allocatedMap);
        return allocatedMap;
    }

    public void calculateNodeLoad(Map<Integer, List<Integer>> allocatedMap) {

        // (nodeId, (partitionId, Shuffle下游生成的Partition存在于该节点中的数据量))
        Map<Integer, Map<Integer, Integer>> nodeContainsPartitionItem = new HashMap<>();

        // (partitionId, (nodeId, Shuffle下游生成的Partition分布在该计算节点中的记录数))
        Map<Integer, Map<Integer, Integer>> partitionItemOnNode = new HashMap<>();

        // 计算每个节点中包含各个下游生成Partition的数据量
        for (int nodeId = 0; nodeId < nodeNum; nodeId++) {
            if (!nodeContainsPartitionItem.containsKey(nodeId)) {
                nodeContainsPartitionItem.put(nodeId, new HashMap<>());
            }
            // 遍历该计算节点上的所有初始分配的Partition，获取每个Partition中的键值Key分布，计算重新分区后该计算节点包含新分区中的键值Key的记录数
            List<Integer> partitions = initialPartitionDistribution.get(nodeId);
            if (partitions == null || partitions.isEmpty()) {
                continue;
            }
            for (int originalPartitionId : partitions) {
                // 获取Partition中的键值Key分布
                Map<String, Integer> keyCountMap = originalKeyDistribution.get(originalPartitionId);
                for (Map.Entry<String, Integer> entry : keyCountMap.entrySet()) {
                    String key = entry.getKey();
                    int keySize = entry.getValue();
                    // 获取Key在Shuffle下游的partitionId
                    int currentPartitionId = keyInPartition.getOrDefault(key, -1);
                    if (currentPartitionId == -1) {
                        currentPartitionId = key.hashCode() % partitionNum;
                        if (currentPartitionId < 0) {
                            currentPartitionId += partitionNum;
                        }
                    }
                    // 记录计算节点包含重新分区后的数据量
                    nodeContainsPartitionItem.get(nodeId).put(currentPartitionId,
                            nodeContainsPartitionItem.get(nodeId).getOrDefault(currentPartitionId, 0) + keySize);

                    // 记录下游Partition分布在各计算节点中的记录数
                    if (!partitionItemOnNode.containsKey(currentPartitionId)) {
                        partitionItemOnNode.put(currentPartitionId, new HashMap<>());
                    }
                    partitionItemOnNode.get(currentPartitionId).put(nodeId,
                            partitionItemOnNode.get(currentPartitionId).getOrDefault(nodeId, 0) + keySize);
                }
            }
        }

        Map<Integer, Double> nodeLoadMap = new HashMap<>(initialLoadMap);
        allocatedMap.forEach((nodeId, partitionList) -> {
            // 遍历分布在该计算节点上的所有Partition
            partitionList.forEach(partitionId -> {
                // 获取该Partition分布在各个计算节点中的数据
                Map<Integer, Integer> itemOnNode = partitionItemOnNode.get(partitionId);
                itemOnNode.forEach((nodeId2, itemSize) -> {
                    double cost = 0.0;
                    if (nodeId2.equals(nodeId)) {
                        cost = itemSize * localCostPerItem;
                    } else {
                        cost = itemSize * remoteCostPerItem;
                    }
                    nodeLoadMap.put(nodeId, Double.sum(nodeLoadMap.getOrDefault(nodeId, 0d), cost));
                });
            }) ;
        });

        System.out.println(nodeLoadMap);
    }

}
