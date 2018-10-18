package com.dmy.graduation.core;

import com.dmy.graduation.partitioner.DSPartitionerMock;
import com.dmy.graduation.partitioner.HashPartitionerMock;
import com.dmy.graduation.partitioner.RangePartitionerMock;
import com.dmy.graduation.util.FileUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(locations = "classpath*:/applicationTest.xml")
public class ApplicationTest {

    @Autowired
    private FileUtil fileUtil;

    @Autowired
    private HashPartitionerMock hashPartitionerMock;

    @Autowired
    private RangePartitionerMock rangePartitionerMock;

    @Autowired
    private DSPartitionerMock dsPartitionerMock;

    private Map<String, Integer> appVisitCountMap;

    @Before
    public void init() {
        fileUtil.initAppVisitCount();
        appVisitCountMap = fileUtil.getAppVisitCountMap();
    }

    @Test
    public void contextLoads() {
        appVisitCountMap.forEach((key, count) -> System.out.println(key + ": " + count));
    }

    @Test
    public void testHashPartitioner() {
        hashPartitionerMock.setPartitionNum(200);
        hashPartitionerMock.setKeyCountMap(appVisitCountMap);
        System.out.println(hashPartitionerMock.calculateBalanceRate());
        int[] countArray = new int[2];
        countArray[0] = Integer.MIN_VALUE;
        countArray[1] = Integer.MAX_VALUE;
        hashPartitionerMock.getPartitionPairCountMap().forEach((partitionId, count) -> {
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
    public void testDSPartitioner() {
        dsPartitionerMock.setPartitionNum(80);
        dsPartitionerMock.setKeyCountMap(appVisitCountMap);
        System.out.println(dsPartitionerMock.calculateBalanceRate());
        dsPartitionerMock.getPartitionPairCountMap().forEach((partitionId, count) ->
                System.out.println(partitionId + ": " + count + " " + dsPartitionerMock.getPartitionKeyMap().get(partitionId)));
    }

    public void testRangePartitioner() {
        // 先利用哈希分区来获取最初的数据分区方式
        hashPartitionerMock.setPartitionNum(30);
        hashPartitionerMock.setKeyCountMap(appVisitCountMap);
        hashPartitionerMock.calculateBalanceRate();

        Map<Integer, List<String>> originalPartitionKeyMap = hashPartitionerMock.getPartitionKeyMap();

    }

    @Test
    public void testPartitioner() {
        for (int partitionNum = 30; partitionNum <= 200; partitionNum += 10) {
            System.out.println("============================" + partitionNum + "==============================");
            System.out.println("HashPartitioner:");
            hashPartitionerMock.setPartitionNum(partitionNum);
            hashPartitionerMock.setKeyCountMap(appVisitCountMap);
            System.out.println("不均衡度：" + hashPartitionerMock.calculateBalanceRate());
            int[] countArray1 = new int[3];
            countArray1[0] = Integer.MIN_VALUE;
            countArray1[1] = Integer.MAX_VALUE;
            countArray1[2] = 0;
            hashPartitionerMock.getPartitionPairCountMap().forEach((partitionId, count) -> {
                if (count == 0) {
                    countArray1[2]++;
                }
                if (count > countArray1[0]) {
                    countArray1[0] = count;
                }
                if (count < countArray1[1]) {
                    countArray1[1] = count;
                }
                //System.out.println(partitionId + ": " + count + " " + hashPartitionerMock.getPartitionKeyMap().get(partitionId));
            });
            System.out.println("极差：" + (countArray1[0] - countArray1[1]));
            System.out.println("空闲partition数目：" + countArray1[2]);

            System.out.println();
            System.out.println("DSPartitioner:");
            dsPartitionerMock.setPartitionNum(partitionNum);
            dsPartitionerMock.setKeyCountMap(appVisitCountMap);
            System.out.println("不均衡度：" + dsPartitionerMock.calculateBalanceRate());
            int[] countArray2 = new int[3];
            countArray2[0] = Integer.MIN_VALUE;
            countArray2[1] = Integer.MAX_VALUE;
            countArray2[2] = 0;
            dsPartitionerMock.getPartitionPairCountMap().forEach((partitionId, count) -> {
                if (count == 0) {
                    countArray2[2]++;
                }
                if (count > countArray2[0]) {
                    countArray2[0] = count;
                }
                if (count < countArray2[1]) {
                    countArray2[1] = count;
                }
                //System.out.println(partitionId + ": " + count + " " + hashPartitionerMock.getPartitionKeyMap().get(partitionId));
            });
            System.out.println("极差：" + (countArray2[0] - countArray2[1]));
            System.out.println("空闲partition数目：" + countArray2[2]);

            System.out.println("==========================================================");
        }
    }
}
