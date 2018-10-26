package com.dmy.graduation.partitioner.mock;

import java.math.BigDecimal;
import java.util.*;

/**
 * Created by DMY on 2018/10/10 15:26
 */
public class RangePartitionerMock implements PartitionerMock {

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
    private Map<Integer, List<String>> originalPartitionKeyMap;

    /**
     * 对数据进行重新数据分区后的结果
     */
    private Map<Integer, List<String>> rePartitionKeyMap;
    private Map<Integer, Integer> rePartitionSizeMap;

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

    public RangePartitionerMock() {
    }

    public RangePartitionerMock(int partitionNum, Map<String, Integer> keyCountMap, Map<Integer, List<String>> originalPartitionKeyMap) {
        this.partitionNum = partitionNum;
        this.keyCountMap = keyCountMap;
        this.originalPartitionKeyMap = originalPartitionKeyMap;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public void setOriginalPartitionKeyMap(Map<Integer, List<String>> originalPartitionKeyMap) {
        this.originalPartitionKeyMap = originalPartitionKeyMap;
    }

    public void setKeyCountMap(Map<String, Integer> keyCountMap) {
        this.keyCountMap = keyCountMap;
    }

    public Map<Integer, List<String>> getRePartitionKeyMap() {
        return rePartitionKeyMap;
    }

    public Map<Integer, Integer> getRePartitionSizeMap() {
        return rePartitionSizeMap;
    }

    /**
     * 计算不均衡度
     */
    public double calculateTiltRate() {

        assert (partitionNum > 0 && keyCountMap != null);

        // 计算整体采样数据规模,其中partitionNum表示子RDD中包含的partition个数
        double sampleSize = Math.min(10000.0 * partitionNum, 1e6);

        // 计算待采样RDD中每个partition应该采集的样本数，这里乘以3是为了后续判断一个partition中是否发生了数据倾斜
        int sampleSizePerPartition = (int) Math.ceil(3.0 * sampleSize / originalPartitionKeyMap.keySet().size());

        // 遍历每个partition中的数据进行抽样
        List<Triple> sketched = new ArrayList<>();
        originalPartitionKeyMap.forEach((partitionId, keyList) -> {
            Triple triple = sketch(partitionId, keyList, sampleSizePerPartition);
            sketched.add(triple);
        });

        // 计算采样比例
        double fraction = Math.min(sampleSize / Math.max(totalCount, 1), 1.0);

        // 获取最终采样数据
        List<Tuple> candidates = getCandidates(fraction, sampleSizePerPartition, sketched);

        // 获取每个partition包含的key边界
        Map<Integer, List<String>> bounds = determineBounds(candidates);

        // 对原有的数据进行重新分区并计算每个分区包含的键值对个数
        rePartitionKeyMap = rePartition(bounds);
        rePartitionSizeMap = new HashMap<>(partitionNum);
        rePartitionKeyMap.forEach((partitionId, keyList) ->
                keyList.forEach(key ->
                        rePartitionSizeMap.put(partitionId, rePartitionSizeMap.getOrDefault(partitionId, 0) + keyCountMap.getOrDefault(key, 0))));

        // 计算不均衡度
        double avgPartitionCount = (double) totalCount / (double) partitionNum;
        double totalDeviation = 0.0;
        for (int i = 0; i < partitionNum; i++) {
            double deviation = avgPartitionCount - rePartitionSizeMap.getOrDefault(i, 0);
            totalDeviation += Math.pow(deviation, 2);
        }
        double partitionBalanceRate = Math.sqrt(totalDeviation / (double) (partitionNum - 1)) / avgPartitionCount;
        BigDecimal bigDecimal = new BigDecimal(partitionBalanceRate);
        return bigDecimal.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * 采样数据
     *
     * @param partitionId   partition标识
     * @param partitionKeys 该partition中包含的key集合
     * @param sampleCount   该partition采样的数据量
     * @return partition采样结果
     */
    private Triple sketch(int partitionId, List<String> partitionKeys, int sampleCount) {

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

        if (keyList.size() <= sampleCount) {
            return keyList;
        }
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
     * 获得最终的采样数据，这里包括对倾斜partition的重新抽样
     *
     * @param fraction               整体的数据采样比例
     * @param sampleSizePerPartition 每个partition中采样的键值对数目
     * @param sketched               初始采样数据（三元组）
     * @return 最终采样数据（二元组）
     */
    private List<Tuple> getCandidates(double fraction, int sampleSizePerPartition, List<Triple> sketched) {

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
                Triple triple = sketch(partitionSampleInfo.partitionId, originalPartitionKeyMap.get(partitionSampleInfo.partitionId),
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

        return candidates;
    }

    /**
     * 确定每个Partition中Key的范围
     */
    private Map<Integer, List<String>> determineBounds(List<Tuple> candidates) {

        // 对candidates中的相同key进行weight合并，最终按照key进行排序
        Map<String, Tuple> map = new HashMap<>();
        candidates.forEach(tuple -> {
            if (!map.containsKey(tuple.key)) {
                map.put(tuple.key, tuple);
            } else {
                tuple.weight += map.get(tuple.key).weight;
                map.put(tuple.key, tuple);
            }
        });
        candidates = new ArrayList<>(map.values());
        candidates.sort((tuple1, tuple2) -> {
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
     * 对原始的RDD中各分区的数据进行重新分区
     *
     * @param bounds 各分区边界
     * @return 重新分区后的结果
     */
    private Map<Integer, List<String>> rePartition(Map<Integer, List<String>> bounds) {
        Map<Integer, List<String>> curPartitionKeyMap = new HashMap<>();
        originalPartitionKeyMap.forEach((id, keyList) -> {
            keyList.forEach(key -> {
                int partitionId = getPartition(key, bounds);
                if (!curPartitionKeyMap.containsKey(partitionId)) {
                    curPartitionKeyMap.put(partitionId, new ArrayList<>());
                }
                curPartitionKeyMap.get(partitionId).add(key);
            });
        });
        return curPartitionKeyMap;
    }

    /**
     * 获得key所在的partition编号
     *
     * @param key    待分区的key
     * @param bounds 各分区边界
     * @return 该key应当所在partition
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
