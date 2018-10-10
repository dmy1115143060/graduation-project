package com.dmy.graduation.partitioner;

import java.util.*;

/**
 * Created by DMY on 2018/10/10 15:26
 */
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

    public RangePartitionerMock(int partitionNum, Map<String, Integer> keyCountMap,
                                Map<Integer, List<String>> partitionKeyMap) {
        this.partitionNum = partitionNum;
        this.keyCountMap = keyCountMap;
        this.partitionKeyMap = partitionKeyMap;
    }

    public double calculateBalanceRate() {
        assert (partitionNum > 0 && keyCountMap != null);

        // 计算整体采样数据规模,其中partitionNum表示子RDD中包含的partition个数
        double sampleSize = Math.min(20.0 * partitionNum, 1e6);

        // 计算待采样RDD中每个partition应该采集的样本数，这里乘以3是为了后续判断一个partition中是否发生了数据倾斜
        int sampleSizePerPartition = (int) Math.ceil(3.0 * sampleSize / partitionKeyMap.keySet().size());

        // 遍历每个partition中的数据进行抽样
        List<Triple> partitionSampleData = new ArrayList<>();
        partitionKeyMap.forEach((partitionId, keyList) -> {
            Triple triple = sketch(partitionId, keyList, sampleSizePerPartition);
            partitionSampleData.add(triple);
        });

        return 0d;
    }

    /**
     * 采样数据
     * @param partitionId partition标识
     * @param partitionKeys 该partition中包含的key集合
     * @param sampleCount 该partition采样的数据量
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

        // 打乱数据
        Collections.shuffle(keyList);

        // 水塘抽样抽取数据
        List<String> sampleKeyList = reservoirSample(keyList, sampleCount);

        Triple triple = new Triple();
        triple.setPartitionId(partitionId);
        triple.setPartitionId(partitionId);
        triple.setPartitionSize(partitionSize);
        triple.setSampleList(sampleKeyList);

        return triple;
    }

    /**
     * 水塘抽样
     * @param keyList 待采样数据集合
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
     * 三元组：<partition标识，partition中包含的键值对个数，抽样的Key>
     */
    class Triple {
        int partitionId;
        int partitionSize;
        List<String> sampleList;

        Triple() {

        }

        void setPartitionId(int partitionId) {
            this.partitionId = partitionId;
        }

        void setPartitionSize(int partitionSize) {
            this.partitionSize = partitionSize;
        }

        void setSampleList(List<String> sampleList) {
            this.sampleList = sampleList;
        }
    }
}
