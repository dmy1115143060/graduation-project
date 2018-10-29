package com.dmy.graduation.partitioner.mock;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Created by DMY on 2018/10/27 19:08
 */
public class DefaultPartitionerMockImpl implements PartitionerMock {

    @Override
    public double calculateTiltRate(Map<Integer, Long> partitionSizeMap, int partitionNum) {
        long totalSize = 0L;
        for (Map.Entry<Integer, Long> entry : partitionSizeMap.entrySet()) {
            totalSize += entry.getValue();
        }
        long avgPartitionSize = totalSize / partitionNum;
        double totalDeviation = 0.0;
        for (int i = 0; i < partitionNum; i++) {
            double deviation = avgPartitionSize - partitionSizeMap.getOrDefault(i, 0L);
            totalDeviation += Math.pow(deviation, 2);
        }
        double tiltDegree = Math.sqrt(totalDeviation / (double) (partitionNum - 1)) / avgPartitionSize;
        BigDecimal bigDecimal = new BigDecimal(tiltDegree);
        return bigDecimal.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
