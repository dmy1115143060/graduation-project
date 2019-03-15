package com.dmy.graduation.core;

import com.dmy.graduation.util.ZipfGenerator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by DMY on 2019/2/22 11:04
 */
public class ZipfGeneratorTest extends BaseTest {

    @Autowired
    private ZipfGenerator zipfGenerator;

    @Test
    public void testGenerator() {
        Map<Integer, Integer> countMap = new HashMap<>(zipfGenerator.getSize());
        for (int i = 1; i <= zipfGenerator.getSize(); i++) {
            int key = zipfGenerator.next();
            countMap.put(key, countMap.getOrDefault(key, 0) + 1);
        }
        System.out.println(countMap);
    }
}
