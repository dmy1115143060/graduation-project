package com.dmy.graduation.partitioner;

import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Created by DMY on 2018/10/10 15:26
 */
@Service
public class RangePartitionerMock {

    /**
     * 分区个数
     */
    private int partitionNum;

    /**
     * 待采样RDD中的键值对个数
     */
    private int totalCount;

    /**
     * key: 关键字   value: 关键字出现次数
     */
    private Map<String, Integer> keyCountMap;

    /**
     * key: partitionId   value: 该partition包含的key
     * 需要对此RDD进行数据采样
     */
    private Map<Integer, List<String>> partitionKeyMap;

    /**
     * 二元组：<key，key的权重>
     */
    class Tuple {
        String key;
        double weight;
    }

    /**
     * 三元组：<partition标识，partition中包含的键值对个数，抽样的Key>
     */
    class Triple {
        int partitionId;
        int partitionSize;
        List<String> sampleList;
    }

    public RangePartitionerMock(int partitionNum, Map<String, Integer> keyCountMap, Map<Integer, List<String>> partitionKeyMap) {
        this.partitionNum = partitionNum;
        this.keyCountMap = keyCountMap;
        this.partitionKeyMap = partitionKeyMap;
    }

    /**
     * 计算不均衡度
     */
    public double calculateBalanceRate() {
        assert (partitionNum > 0 && keyCountMap != null);

        // 计算整体采样数据规模,其中partitionNum表示子RDD中包含的partition个数
        double sampleSize = Math.min(20.0 * partitionNum, 1e6);

        // 计算待采样RDD中每个partition应该采集的样本数，这里乘以3是为了后续判断一个partition中是否发生了数据倾斜
        int sampleSizePerPartition = (int) Math.ceil(3.0 * sampleSize / partitionKeyMap.keySet().size());

        // 遍历每个partition中的数据进行抽样
        List<Triple> sketched = new ArrayList<>();
        partitionKeyMap.forEach((partitionId, keyList) -> {
            Triple triple = sketch(partitionId, keyList, sampleSizePerPartition);
            sketched.add(triple);
        });

        // 计算采样比例
        double fraction = Math.min(sampleSize / Math.max(totalCount, 1), 1.0);

        // 由于已经知道每个分区中的数据量和采样比例，扫描分区计算按照该采样比例采样出来的数据
        // 是否大于sampleSizePerPartition。若大于则认为该分区是倾斜的，需要按照采样比例重新进行采样
        List<Tuple> candidates = new ArrayList<>();
        Set<Triple> imbalancedPartitions = new HashSet<>();
        sketched.forEach(partitionSampleInfo -> {
            if (fraction * partitionSampleInfo.partitionSize > sampleSizePerPartition) {
                imbalancedPartitions.add(partitionSampleInfo);
            } else {
                double weight = (double) partitionSampleInfo.partitionSize / (double) partitionSampleInfo.sampleList.size();
                partitionSampleInfo.sampleList.forEach(key -> {
                    Tuple tuple = new Tuple();
                    tuple.key = key;
                    tuple.weight = weight;
                    candidates.add(tuple);
                });
            }
        });

        // 对非均衡的partition重新抽样
        List<Triple> reSketched = new ArrayList<>();
        if (!imbalancedPartitions.isEmpty()) {
            imbalancedPartitions.forEach(partitionSampleInfo -> {
                Triple triple = sketch(partitionSampleInfo.partitionId, partitionKeyMap.get(partitionSampleInfo.partitionId),
                        (int) (partitionSampleInfo.partitionSize * fraction));
                reSketched.add(triple);
            });
        }
        reSketched.forEach(partitionSampleInfo -> {
            double weight = (1.0) / fraction;
            partitionSampleInfo.sampleList.forEach(key -> {
                Tuple tuple = new Tuple();
                tuple.key = key;
                tuple.weight = weight;
                candidates.add(tuple);
            });
        });
        return 0d;
    }

    /**
     * 采样数据
     *
     * @param partitionId   partition标识
     * @param partitionKeys 该partition中包含的key集合
     * @param sampleCount   该partition采样的数据量
     * @return partition采样结果
     */
    public Triple sketch(int partitionId, List<String> partitionKeys, int sampleCount) {
        // 由于已经知道每个partition中包含的key，因此进行模拟，即先将一个partition
        // 中的key按照出现次数加入到集合中，然后进行数据混洗，最终再进行水塘抽样
        int partitionSize = 0;
        List<String> keyList = new ArrayList<>();
        for (String key : partitionKeys) {
            // 获取该key对应的键值对个数
            int count = keyCountMap.getOrDefault(key, 0);
            partitionSize += count;
            if (count != 0) {
                for (int i = 0; i < count; i++) {
                    keyList.add(key);
                }
            }
        }
        totalCount += partitionSize;

        // 打乱数据
        Collections.shuffle(keyList);

        // 水塘抽样抽取数据
        List<String> sampleKeyList = reservoirSample(keyList, sampleCount);

        Triple triple = new Triple();
        triple.partitionId = partitionId;
        triple.partitionSize = partitionSize;
        triple.sampleList = sampleKeyList;
        return triple;
    }

    /**
     * 水塘抽样
     *
     * @param keyList     待采样数据集合
     * @param sampleCount 采样的数据量
     * @return 采样结果
     */
    private List<String> reservoirSample(List<String> keyList, int sampleCount) {
        String[] sampleKeys = new String[sampleCount];
        Random random = new Random();
        int k = 0;
        for (String key : keyList) {
            if (k < sampleCount) {
                sampleKeys[k] = key;
                k++;
            } else {
                int index = random.nextInt(sampleCount);
                sampleKeys[index] = key;
            }
        }
        return Arrays.asList(sampleKeys);
    }

    /**
     * 确定每个Partition中Key的范围
     *
     * @return
     */
    private Map<Integer, List<String>> determineBounds(List<Tuple> candidates) {
        // 先对采样数据按照Key进行排序
        Collections.sort(candidates, (tuple1, tuple2) -> {
            String key1 = tuple1.key;
            String key2 = tuple2.key;
            return key1.compareTo(key2);
        });

        // 计算所有采样Key的权重和以及每个Partition的平均权重值
        double totalWeight = candidates.stream().map(tuple -> tuple.weight).reduce(0d, (acc, value) -> acc + value);
        double avgPartitionWeight = totalWeight / partitionNum;

        // 边界划分,每次将采样的key尝试放入第i个partition中（0 <= i <= partitionNum）
        // 放入的条件是当前partition中Key的权重和小于partition的平均权重值
        Map<Integer, List<String>> bounds = new HashMap<>();
        Map<Integer, Double> partitionWeightMap = new HashMap<>();
        int i = 0;
        int j = 0;
        int size = candidates.size();
        while (i < partitionNum && j < size) {
            if (!bounds.containsKey(i)) {
                bounds.put(i, new ArrayList<>());
            }
            String key = candidates.get(j).key;
            double weight = candidates.get(j).weight;
            if (partitionWeightMap.getOrDefault(i, 0d).compareTo(avgPartitionWeight) < 0) {
                bounds.get(i).add(key);
                partitionWeightMap.put(i, partitionWeightMap.getOrDefault(i, 0d) + weight);
                j++;
            } else {
                i++;
            }
        }
        return bounds;
    }

    /**
     * 获得key所在的partition编号
     */
    private int getPartition(String key, Map<Integer, List<String>> bounds) {
        for (int i = 0; i < partitionNum; i++) {
            List<String> keys = bounds.get(i);
            if (keys != null && !keys.isEmpty()) {
                String maxKey = keys.get(keys.size() - 1);
                String minKey = keys.get(0);
                if (minKey.compareTo(key) <= 0 && maxKey.compareTo(key) >= 0) {
                    return i;
                }
            } else {
                return i;
            }
        }
        return 0;
    }
}
