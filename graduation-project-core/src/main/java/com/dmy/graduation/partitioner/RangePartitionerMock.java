package com.dmy.graduation.partitioner;

import java.util.Map;

/**
 * Created by DMY on 2018/10/10 15:26
 */
public class RangePartitionerMock {

    /**
     * key: 关键字   value: 关键字出现次数
     */
    private Map<String, Integer> keyCountMap;

    public RangePartitionerMock(Map<String, Integer> keyCountMap) {
        this.keyCountMap = keyCountMap;
    }
}
