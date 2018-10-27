package com.dmy.graduation.core;

import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by DMY on 2018/10/26 21:05
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(locations = "classpath*:/applicationTest.xml")
public class Test {

    @org.junit.Test
    public void testKeyCount() {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("G:\\Intellij\\TPCDSkew\\lineitem.tbl"));
            String line = null;
            Map<String, Integer> map = new HashMap<>();
            while ((line = bufferedReader.readLine()) != null) {
                String[] splits = line.split("\\|");
                String key = splits[2];
                map.put(key, map.getOrDefault(key, 0) + 1);
            }
            int maxCount = Integer.MIN_VALUE;
            String maxCountKey = null;
            int minCount = Integer.MAX_VALUE;
            String minCountKey = null;
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    maxCountKey = entry.getKey();
                }
                if (entry.getValue() < minCount) {
                    minCount = entry.getValue();
                    minCountKey = entry.getKey();
                }
            }
            System.out.println("=============================================");
            System.out.println(maxCountKey + ": " + maxCount);
            System.out.println(minCountKey + ": " + minCount);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
