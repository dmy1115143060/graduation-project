package com.dmy.graduation.core;

import com.dmy.graduation.partitioner.mock.DefaultPartitionerMockImpl;
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

    private static final int PARTITION_NUM = 30;

    private static final String RESOURCE_FILE_PATH = "G:\\Intellij\\graduation-project\\graduation-project-core\\src\\main\\resources\\files\\tpch";

    @Test
    public void testTilt() {
       // String[] foldNames = {"1.0", "1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7", "1.8", "1.9", "2.0"};
        String[] foldNames = {"1.0", "1.3", "1.6"};
        String[] partitionerFileNames = {"Hash_PartitionSize.txt", "Range_PartitionSize.txt", "DS_PartitionSize.txt"};
        for (int i = 0; i < foldNames.length; i++) {
            System.out.println(foldNames[i]);
            for (int j = 0; j < partitionerFileNames.length; j++) {
                Map<Integer, Long> partitionSizeMap = getPartitionSizeMap(foldNames[i], partitionerFileNames[j]);
                System.out.println(defaultPartitionerMock.calculateTiltRate(partitionSizeMap, PARTITION_NUM));
            }
            System.out.println("\n\n");
        }
    }

    private Map<Integer, Long> getPartitionSizeMap(String foldName, String partitionerFileName) {
        Map<Integer, Long> partitionSizeMap = new HashMap<>(PARTITION_NUM);
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(RESOURCE_FILE_PATH + "\\20G\\" + foldName + "\\" + partitionerFileName));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] splits = line.split(":");
                partitionSizeMap.put(Integer.parseInt(splits[0]), Long.parseLong(splits[1]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return partitionSizeMap;
    }
}
