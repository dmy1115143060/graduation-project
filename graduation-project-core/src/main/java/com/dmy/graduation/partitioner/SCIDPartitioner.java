package com.dmy.graduation.partitioner;

import org.apache.spark.Partitioner;

import java.util.Map;

/**
 * Created by DMY on 2018/10/31 13:50
 */
public class SCIDPartitioner extends Partitioner {

    /**
     * key: key_partitionId, value: 数量
     * 表示该key分配给该partitionId中的数量
     */
    private Map<String, Integer> keyPartitionTrackMap;

    private int partitionNum;

    public SCIDPartitioner(Map<String, Integer> keyPartitionTrackMap) {
        this.keyPartitionTrackMap = keyPartitionTrackMap;
    }

    @Override
    public int numPartitions() {
        return partitionNum;
    }

    @Override
    public int getPartition(Object key) {
        String k = (String) key;
        int partitionId = 0;
        if (!keyPartitionTrackMap.containsKey(k)) {
            partitionId = key.hashCode() % partitionNum;
            if (partitionId < 0) {
                partitionId += partitionNum;
            }
        }
        return partitionId;
    }
}
