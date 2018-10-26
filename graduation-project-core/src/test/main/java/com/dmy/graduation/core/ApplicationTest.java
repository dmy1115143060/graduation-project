package com.dmy.graduation.core;

import com.dmy.graduation.partitioner.mock.DSPartitionerMock;
import com.dmy.graduation.partitioner.mock.HashPartitionerMock;
import com.dmy.graduation.partitioner.mock.RangePartitionerMock;
import com.dmy.graduation.util.AppFileUtil;
import com.dmy.graduation.util.FileUtil;
import com.dmy.graduation.util.TPCHFileUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(locations = "classpath*:/applicationTest.xml")
public class ApplicationTest {

    @Autowired
    private AppFileUtil appFileUtil;

    @Autowired
    private TPCHFileUtil tpchFileUtil;

    @Autowired
    private FileUtil fileUtil;

    @Autowired
    private HashPartitionerMock hashPartitionerMock;

    @Autowired
    private RangePartitionerMock rangePartitionerMock;

    @Autowired
    private DSPartitionerMock dsPartitionerMock;

    private Map<String, Integer> keyCountMap;

    @Before
    public void init() {
        fileUtil.initKeyCount("lineItemSuppKeyCount.txt");
        keyCountMap = fileUtil.getKeyCountMap();
    }

    @Test
    public void testAppFileUtil() {
        appFileUtil.generateAppVisitCount();
    }

    @Test
    public void testTPCHFileUtil() {
        tpchFileUtil.generateKeyCount("F:\\研究内容相关资料\\experiment_data\\lineitem.tbl");
    }

    @Test
    public void testHashPartitioner() {
        hashPartitionerMock.setPartitionNum(200);
        hashPartitionerMock.setKeyCountMap(keyCountMap);
        System.out.println(hashPartitionerMock.calculateTiltRate());
        int[] countArray = new int[2];
        countArray[0] = Integer.MIN_VALUE;
        countArray[1] = Integer.MAX_VALUE;
        hashPartitionerMock.getPartitionSizeMap().forEach((partitionId, count) -> {
            if (count > countArray[0]) {
                countArray[0] = count;
            }
            if (count < countArray[1]) {
                countArray[1] = count;
            }
            //System.out.println(partitionId + ": " + count + " " + hashPartitionerMock.getPartitionKeyMap().get(partitionId));
        });
        System.out.println(countArray[0] - countArray[1]);
    }

    @Test
    public void testRangePartitioner() {
        // 先利用哈希分区来获取最初的数据分区方式
        int partitionNum = 30;
        hashPartitionerMock.setPartitionNum(partitionNum);
        hashPartitionerMock.setKeyCountMap(keyCountMap);
        hashPartitionerMock.calculateTiltRate();

        Map<Integer, List<String>> originalPartitionKeyMap = hashPartitionerMock.getPartitionKeyMap();
        rangePartitionerMock.setPartitionNum(partitionNum);
        rangePartitionerMock.setKeyCountMap(keyCountMap);
        rangePartitionerMock.setOriginalPartitionKeyMap(originalPartitionKeyMap);
        System.out.println("不均衡度：" + rangePartitionerMock.calculateTiltRate());
        rangePartitionerMock.getRePartitionSizeMap().forEach((partitionId, size) ->
                System.out.println("partitionId: " + partitionId + " size: " + size + " "
                        + rangePartitionerMock.getRePartitionKeyMap().get(partitionId)));
    }

    @Test
    public void testDSPartitioner() {
        dsPartitionerMock.setPartitionNum(80);
        dsPartitionerMock.setKeyCountMap(keyCountMap);
        System.out.println(dsPartitionerMock.calculateTiltRate());
        dsPartitionerMock.getPartitionSzieMap().forEach((partitionId, count) ->
                System.out.println(partitionId + ": " + count + " " + dsPartitionerMock.getPartitionKeyMap().get(partitionId)));
    }

    @Test
    public void testPartitioner() {
        Map<Integer, List<Object>> hashPartitionerDataMap = new HashMap<>();
        Map<Integer, List<Object>> rangePartitionerDataMap = new HashMap<>();
        Map<Integer, List<Object>> dsPartitionerDataMap = new HashMap<>();
        int minPartitionNum = 30;
        int maxPartitionNum = 60;
        int step = 1;

        for (int partitionNum = minPartitionNum; partitionNum <= maxPartitionNum; partitionNum += step) {
//            System.out.println("============================" + partitionNum + "==============================");
//            System.out.println("HashPartitioner:");
            hashPartitionerMock.setPartitionNum(partitionNum);
            hashPartitionerMock.setKeyCountMap(keyCountMap);
            double inBalanceRate1 = hashPartitionerMock.calculateTiltRate();
            hashPartitionerDataMap.put(partitionNum, new ArrayList<>());
            hashPartitionerDataMap.get(partitionNum).add(inBalanceRate1);
//            System.out.println("不均衡度：" + hashPartitionerMock.calculateBalanceRate());
            int[] countArray1 = new int[3];
            countArray1[0] = Integer.MIN_VALUE;
            countArray1[1] = Integer.MAX_VALUE;
            countArray1[2] = 0;
            for (int i = 0; i < partitionNum; i++) {
                int size = hashPartitionerMock.getPartitionSizeMap().getOrDefault(i, 0);
                if (size == 0) {
                    countArray1[2]++;
                }
                if (size > countArray1[0]) {
                    countArray1[0] = size;
                }
                if (size < countArray1[1]) {
                    countArray1[1] = size;
                }
            }
            hashPartitionerDataMap.get(partitionNum).add(countArray1[0] - countArray1[1]);
            hashPartitionerDataMap.get(partitionNum).add(countArray1[2]);
//            System.out.println("极差：" + (countArray1[0] - countArray1[1]));
//            System.out.println("空闲partition数目：" + countArray1[2]);

//            System.out.println();
//            System.out.println("DSPartitioner:");
            dsPartitionerMock.setPartitionNum(partitionNum);
            dsPartitionerMock.setKeyCountMap(keyCountMap);
            double inBalanceRate2 = dsPartitionerMock.calculateTiltRate();
            dsPartitionerDataMap.put(partitionNum, new ArrayList<>());
            dsPartitionerDataMap.get(partitionNum).add(inBalanceRate2);
//            System.out.println("不均衡度：" + dsPartitionerMock.calculateBalanceRate());
            int[] countArray2 = new int[3];
            countArray2[0] = Integer.MIN_VALUE;
            countArray2[1] = Integer.MAX_VALUE;
            countArray2[2] = 0;
            for (int i = 0; i < partitionNum; i++) {
                int size = dsPartitionerMock.getPartitionSzieMap().getOrDefault(i, 0);
                if (size == 0) {
                    countArray2[2]++;
                }
                if (size > countArray2[0]) {
                    countArray2[0] = size;
                }
                if (size < countArray2[1]) {
                    countArray2[1] = size;
                }
            }
            dsPartitionerDataMap.get(partitionNum).add(countArray2[0] - countArray2[1]);
            dsPartitionerDataMap.get(partitionNum).add(countArray2[2]);
//            System.out.println("极差：" + (countArray2[0] - countArray2[1]));
//            System.out.println("空闲partition数目：" + countArray2[2]);

//            System.out.println();
//            System.out.println("RangePartitioner:");
            Map<Integer, List<String>> originalPartitionKeyMap = hashPartitionerMock.getPartitionKeyMap();
            rangePartitionerMock.setPartitionNum(partitionNum);
            rangePartitionerMock.setKeyCountMap(keyCountMap);
            rangePartitionerMock.setOriginalPartitionKeyMap(originalPartitionKeyMap);
            double inBalanceRate3 = rangePartitionerMock.calculateTiltRate();
            rangePartitionerDataMap.put(partitionNum, new ArrayList<>());
            rangePartitionerDataMap.get(partitionNum).add(inBalanceRate3);
//            System.out.println("不均衡度：" + rangePartitionerMock.calculateBalanceRate());
            int[] countArray3 = new int[3];
            countArray3[0] = Integer.MIN_VALUE;
            countArray3[1] = Integer.MAX_VALUE;
            countArray3[2] = 0;

            for (int i = 0; i < partitionNum; i++) {
                int size = rangePartitionerMock.getRePartitionSizeMap().getOrDefault(i, 0);
                if (size == 0) {
                    countArray3[2]++;
                }
                if (size > countArray3[0]) {
                    countArray3[0] = size;
                }
                if (size < countArray3[1]) {
                    countArray3[1] = size;
                }
            }
            rangePartitionerDataMap.get(partitionNum).add(countArray3[0] - countArray3[1]);
            rangePartitionerDataMap.get(partitionNum).add(countArray3[2]);
//            System.out.println("极差：" + (countArray3[0] - countArray3[1]));
//            System.out.println("空闲partition数目：" + countArray3[2]);
//            System.out.println();
        }

        for (int i = 0; i <= 2; i++) {
            if (i == 0) {
                System.out.println("分区负载不均衡度：");
            } else if (i == 1) {
                System.out.println("分区极值：");
            } else {
                System.out.println("空闲分区数目：");
            }
            for (int partitionNum = minPartitionNum; partitionNum <= maxPartitionNum; partitionNum += step) {
                System.out.println(partitionNum + "\t"
                        + hashPartitionerDataMap.get(partitionNum).get(i) + "\t"
                        + rangePartitionerDataMap.get(partitionNum).get(i) + "\t"
                        + dsPartitionerDataMap.get(partitionNum).get(i));
            }
            System.out.println("\n\n");
        }
    }
}
