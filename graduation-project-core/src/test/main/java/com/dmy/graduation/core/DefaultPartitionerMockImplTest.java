package com.dmy.graduation.core;

import com.dmy.graduation.partitioner.mock.DefaultPartitionerMockImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by DMY on 2018/10/27 21:34
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(locations = "classpath*:/applicationTest.xml")
public class DefaultPartitionerMockImplTest {

    @Autowired
    private DefaultPartitionerMockImpl defaultPartitionerMock;

    private Map<Integer, Long> partitionSizeMap;

    private int partitionNum;

    @Before
    public void init() {
        partitionNum = 30;
        String RESOURCE_FILE_PATH = "G:\\Intellij\\graduation-project\\graduation-project-core\\src\\main\\resources\\files\\tpch\\10G\\2.0\\";
        partitionSizeMap = new HashMap<>(partitionNum);
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(RESOURCE_FILE_PATH + "DS_PartitionSize.txt"));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] splits = line.split(":");
                partitionSizeMap.put(Integer.parseInt(splits[0]), Long.parseLong(splits[1]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTilt() {
        System.out.println(defaultPartitionerMock.calculateTiltRate(partitionSizeMap, partitionNum));
    }
}
