package com.dmy.graduation.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by DMY on 2018/10/23 15:32
 */
public class FileUtil {

    protected static final int INITIAL_CAPACITY = 1000;
    protected Map<String, Integer> keyCountMap;
    protected static final String RESOURCE_FILE_PATH = "G:\\Intellij\\graduation-project\\graduation-project-core\\src\\main\\resources\\files\\";

    public Map<String, Integer> getKeyCountMap() {
        return keyCountMap;
    }

    public void initKeyCount(String fileName) {
        keyCountMap = new HashMap<>(INITIAL_CAPACITY);
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(RESOURCE_FILE_PATH + fileName));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] splits = line.split(":");
                keyCountMap.put(splits[0], Integer.parseInt(splits[1]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
