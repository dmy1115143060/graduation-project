package com.dmy.graduation.partitioner.mock;

import java.util.Map;

/**
 * Created by DMY on 2018/10/11 1.6:26
 */
public interface PartitionerMock {
    /**
     * 计算倾斜率
     */
    double calculateTiltRate(Map<Integer, Long> partitionSizeMap, int partitionNum);
}
