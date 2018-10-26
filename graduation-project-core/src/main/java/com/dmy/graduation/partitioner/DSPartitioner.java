package com.dmy.graduation.partitioner;

import org.apache.spark.Partitioner;

import java.util.Map;

/**
 * Created by DMY on 2018/10/25 17:24
 */
public class DSPartitioner extends Partitioner {

    private int partitionNum;

    private Map<String, Integer> keyInPartitionMap;

    public DSPartitioner(int partitionNum, Map<String, Integer> keyInPartitionMap) {
        this.partitionNum = partitionNum;
        this.keyInPartitionMap = keyInPartitionMap;
    }

    @Override
    public int getPartition(Object key) {
        String k = (String) key;
        int partitionId = 0;
        if (keyInPartitionMap.containsKey(k)) {
            partitionId =  keyInPartitionMap.get(k);
        } else {
            partitionId = k.hashCode() % numPartitions();
            if (partitionId < 0) {
                partitionId += numPartitions();
            }
        }
        return partitionId;
    }

    @Override
    public int numPartitions() {
        return partitionNum;
    }
}
