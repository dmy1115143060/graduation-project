package com.dmy.graduation.core;

import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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

    @org.junit.Test
    public void generateFile() {
        int MAX = 8000000;
        Random random = new Random();
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("E:\\TPCH_DATA\\KeyWord.txt"));
            int i = 0;
            while (i < MAX) {
                StringBuilder stringBuilder = new StringBuilder();
                for (int j = 0; j < 20; j++) {
                    if (j != 19) {
                        stringBuilder.append("key" + "_" + random.nextInt(MAX) + " ");
                    } else {
                        stringBuilder.append("key" + "_" + random.nextInt(MAX));
                    }
                }
                bufferedWriter.write(stringBuilder.toString());
                bufferedWriter.newLine();
                i++;
                if (i % 1000 == 0) {
                    bufferedWriter.flush();
                }
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
