package com.dmy.graduation.shuffle;

import java.util.*;

/**
 * Created by DMY on 2018/11/5 9:34
 */
public class SCIDOptimization {

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

    public SCIDOptimization() {

    }

    public SCIDOptimization nodeNum(int nodeNum) {
        this.nodeNum = nodeNum;
        return this;
    }

    public SCIDOptimization partitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
        return this;
    }

    public SCIDOptimization localCostPerItem(double localCostPerItem) {
        this.localCostPerItem = localCostPerItem;
        return this;
    }

    public SCIDOptimization remoteCostPerItem(double remoteCostPerItem) {
        this.remoteCostPerItem = remoteCostPerItem;
        return this;
    }

    public SCIDOptimization initialLoadMap(Map<Integer, Double> initialLoadMap) {
        this.initialLoadMap = initialLoadMap;
        return this;
    }

    public SCIDOptimization keyInPartition(Map<String, Integer> keyInPartition) {
        this.keyInPartition = keyInPartition;
        return this;
    }

    public SCIDOptimization originalKeyDistribution(Map<Integer, Map<String, Integer>> originalKeyDistribution) {
        this.originalKeyDistribution = originalKeyDistribution;
        return this;
    }

    public SCIDOptimization initialPartitionDistribution(Map<Integer, List<Integer>> initialPartitionDistribution) {
        this.initialPartitionDistribution = initialPartitionDistribution;
        return this;
    }

    /**
     * 在各节点上面分配Partition (partitionId, 节点标识)
     */
    public Map<Integer, Integer> allocatePartition() {
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

        // 获取初始的Partition分布
        Map<Integer, Integer> initialPartitionPlacement = allocateByLocality(partitionItemOnNode);
        Map<Integer, Integer> allocatedPartition = balanceAllocate(initialPartitionPlacement, partitionItemOnNode, nodeContainsPartitionItem);
        return allocatedPartition;
    }

    /**
     * 按照最优的数据本地性原则分配Partition
     *
     * @param partitionItemDistribution (partitionId, (nodeId, 该Partition分布在该计算节点中的记录数))
     * @return (partitionId, 节点标识)
     */
    private Map<Integer, Integer> allocateByLocality(Map<Integer, Map<Integer, Integer>> partitionItemDistribution) {
        Map<Integer, Integer> partitionDistribution = new HashMap<>();
        for (Map.Entry<Integer, Map<Integer, Integer>> entry1 : partitionItemDistribution.entrySet()) {
            int partitionId = entry1.getKey();
            Map<Integer, Integer> itemDistribution = entry1.getValue();
            int maxSize = -1;
            int allocatedNodeId = -1;
            for (Map.Entry<Integer, Integer> entry2 : itemDistribution.entrySet()) {
                int nodeId = entry2.getKey();
                int size = entry2.getValue();
                if (size > maxSize) {
                    maxSize = size;
                    allocatedNodeId = nodeId;
                }
            }
            if (allocatedNodeId != -1) {
                partitionDistribution.put(partitionId, allocatedNodeId);
            }
        }
        return partitionDistribution;
    }

    /**
     * 负载均衡分配
     *
     * @param initialPartitionPlacement (partitionId, nodeId)
     * @param partitionItemOnNode       (partitionId, (nodeId, Shuffle生成的Partition分布在该计算节点中的记录数))
     * @param nodeContainsPartitionItem (nodeId, (partitionId, Shuffle生成的Partition分布在该计算节点中的记录数))
     * @return (partitionId, nodeId)
     */
    private Map<Integer, Integer> balanceAllocate(Map<Integer, Integer> initialPartitionPlacement,
                                                  Map<Integer, Map<Integer, Integer>> partitionItemOnNode,
                                                  Map<Integer, Map<Integer, Integer>> nodeContainsPartitionItem) {

        // (partitionId, nodeId)
        Map<Integer, Integer> finalPartitionPlacement = new HashMap<>(initialPartitionPlacement);

        // 记录各计算节点对应负载。 (nodeId, 负载)
        Map<Integer, Double> nodeLoadMap = new HashMap<>(initialLoadMap);

        // 记录Partition放置在不同计算节点上所带来的开销。(partitionId, (nodeId, 开销))
        Map<Integer, Map<Integer, Double>> partitionLoadOnNode = new HashMap<>();

        // 按照最优的数据本地性法则对分区进行分配。(nodeId, 分配给该计算节点的分区集合)
        Map<Integer, List<Integer>> partitionAllocatedByLocality = new HashMap<>();
        initialPartitionPlacement.forEach((partitionId, nodeId) -> {
            if (!partitionAllocatedByLocality.containsKey(nodeId)) {
                partitionAllocatedByLocality.put(nodeId, new ArrayList<>());
            }
            partitionAllocatedByLocality.get(nodeId).add(partitionId);
        });

        // 遍历所有的Partition
        for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
            if (!partitionLoadOnNode.containsKey(partitionId)) {
                partitionLoadOnNode.put(partitionId, new HashMap<>());
            }
            Map<Integer, Integer> itemSizeOnNode = partitionItemOnNode.get(partitionId);
            for (int nodeId = 0; nodeId < nodeNum; nodeId++) {
                // 遍历当前Partition分布在各个计算节点中的数据
                for (Map.Entry<Integer, Integer> itemSizeOnNodeEntry : itemSizeOnNode.entrySet()) {
                    int curNodeId = itemSizeOnNodeEntry.getKey();
                    int itemSize = itemSizeOnNodeEntry.getValue();
                    double load = partitionLoadOnNode.get(partitionId).getOrDefault(nodeId, 0d);
                    // 本地读取数据负载
                    if (curNodeId == nodeId) {
                        load += itemSize * localCostPerItem;
                    }
                    // 远程读取数据负载
                    else {
                        load += itemSize * remoteCostPerItem;
                    }
                    partitionLoadOnNode.get(partitionId).put(nodeId, load);
                }
            }
        }

        // 计算初始Partition分配情况下各计算节点的负载
        initialPartitionPlacement.forEach((partitionId, nodeId) -> {
            double load = partitionLoadOnNode.get(partitionId).getOrDefault(nodeId, 0d);
            nodeLoadMap.put(nodeId, nodeLoadMap.getOrDefault(nodeId, 0d) + load);
        });

        // 对计算节点按照负载值大小构造大顶堆
        PriorityQueue<Integer> nodeLoadHeap = new PriorityQueue<>((nodeId1, nodeId2) -> {
            double load1 = nodeLoadMap.getOrDefault(nodeId1, 0d);
            double load2 = nodeLoadMap.getOrDefault(nodeId2, 0d);
            return Double.compare(load2, load1);
        });
        for (int nodeId = 0; nodeId < nodeNum; nodeId++) {
            nodeLoadHeap.add(nodeId);
        }

        System.out.println("初始Partition分配: " + partitionAllocatedByLocality);
        System.out.println("      初始机器负载: " + nodeLoadMap);
        System.out.println("初始makeSpan: " + nodeLoadMap.get(nodeLoadHeap.peek()));
        System.out.println("\n\n");
        int index = 1;
        while (true) {
            // 每次对分配在堆顶计算节点中的Partition集合按照带来的负载进行降序排序
            int maxLoadNodeId = nodeLoadHeap.poll();
            double makeSpan = nodeLoadMap.get(maxLoadNodeId);
            List<Integer> partitionList = partitionAllocatedByLocality.get(maxLoadNodeId);
            if (partitionList == null || partitionList.isEmpty()) {
                break;
            }
            partitionList.sort((partitionId1, partitionId2) -> {
                double load1 = partitionLoadOnNode.get(partitionId1).getOrDefault(maxLoadNodeId, 0d);
                double load2 = partitionLoadOnNode.get(partitionId2).getOrDefault(maxLoadNodeId, 0d);
                return Double.compare(load2, load1);
            });

            // 从最大负载计算节点中获取带来最大负载的Partition
            Integer maxLoadPartitionId = partitionList.get(0);

            // 遍历所有计算节点，选择一个加入该Partition后负载值最小的计算节点
            int allocatedNodeId = -1;
            double minLoad = makeSpan;
            for (int nodeId = 0; nodeId < nodeNum; nodeId++) {
                if (nodeId != maxLoadNodeId) {
                    double load = nodeLoadMap.getOrDefault(nodeId, 0d) + partitionLoadOnNode.get(maxLoadPartitionId).getOrDefault(nodeId, 0d);
                    if (Double.compare(minLoad, load) > 0) {
                        minLoad = load;
                        allocatedNodeId = nodeId;
                    }
                }
            }
            if (allocatedNodeId != -1) {
                System.out.println("==================================Step" + index + "==================================");
                System.out.println("BeforeAllocation_NodeContainsPartition: " + "node_" + maxLoadNodeId + " = "
                        + partitionAllocatedByLocality.get(maxLoadNodeId)
                        + " node_" + allocatedNodeId + " = " + partitionAllocatedByLocality.get(allocatedNodeId));
                System.out.println("BeforeAllocation_NodeLoad：" + "node_" + maxLoadNodeId + " = " + nodeLoadMap.getOrDefault(maxLoadNodeId, 0d)
                        + " node_" + allocatedNodeId + " = " + nodeLoadMap.getOrDefault(allocatedNodeId, 0d));

                // 将Partition带来的负载从最大负载的节点中移除
                partitionAllocatedByLocality.get(maxLoadNodeId).remove(maxLoadPartitionId);
                nodeLoadMap.put(maxLoadNodeId, nodeLoadMap.getOrDefault(maxLoadNodeId, 0d) - partitionLoadOnNode.get(maxLoadPartitionId).getOrDefault(maxLoadNodeId, 0d));

                // 将Partition重新分配并更新负载
                if (!partitionAllocatedByLocality.containsKey(allocatedNodeId)) {
                    partitionAllocatedByLocality.put(allocatedNodeId, new ArrayList<>());
                }
                partitionAllocatedByLocality.get(allocatedNodeId).add(maxLoadPartitionId);
                finalPartitionPlacement.put(maxLoadPartitionId, allocatedNodeId);
                nodeLoadMap.put(allocatedNodeId, nodeLoadMap.getOrDefault(allocatedNodeId, 0d) + partitionLoadOnNode.get(maxLoadPartitionId).getOrDefault(allocatedNodeId, 0d));
            } else {
                break;
            }

            // 更新大顶堆
            nodeLoadHeap.remove(allocatedNodeId);
            nodeLoadHeap.add(allocatedNodeId);
            nodeLoadHeap.add(maxLoadNodeId);

            System.out.println("PartitionAllocation：" + maxLoadPartitionId + " = " + maxLoadNodeId + " -> " + allocatedNodeId);
            System.out.println("AfterAllocation_NodeContainsPartition: " + maxLoadNodeId + " = "
                    + partitionAllocatedByLocality.get(maxLoadNodeId)
                    + " " + allocatedNodeId + " = " + partitionAllocatedByLocality.get(allocatedNodeId));
            System.out.println("AfterAllocation_NodeLoad：" + maxLoadNodeId + " = " + nodeLoadMap.getOrDefault(maxLoadNodeId, 0d)
                    + " " + allocatedNodeId + " = " + nodeLoadMap.getOrDefault(allocatedNodeId, 0d));
            System.out.println("AfterAllocation_MakeSpan：" + "makeSpan = " + makeSpan + " -> " + nodeLoadMap.getOrDefault(nodeLoadHeap.peek(), 0d));
            System.out.println("节点负载: " + nodeLoadMap);
            System.out.println("Partition分配: " + partitionAllocatedByLocality);
            System.out.println("\n\n");
            index++;
        }
        System.out.println("优化后makeSpan：" + nodeLoadMap.get(nodeLoadHeap.peek()));
        return finalPartitionPlacement;
    }
}
