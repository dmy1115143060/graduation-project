package com.dmy.graduation.util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by DMY on 2018/10/23 15:05
 */
public class TPCHFileUtil extends FileUtil {

    public void generateKeyCount(String filePath) {
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            System.out.println("============================数据处理中============================");
            int totalCount = 0;
            while ((line = reader.readLine()) != null) {
                String key = getLineitemKey(line);
                if (key != null) {
                    keyCountMap.put(key, keyCountMap.getOrDefault(key, 0) + 1);
                    totalCount++;
                    if (totalCount % 100000 == 0) {
                        System.out.println("============================已成功处理【" + totalCount + "】条数据============================");
                    }
                }
            }
            writer = new BufferedWriter(new FileWriter(RESOURCE_FILE_PATH + "\\lineItemSuppKeyCount.txt"));
            for (Map.Entry<String, Integer> entry : keyCountMap.entrySet()) {
                writer.write(entry.getKey() + ":" + entry.getValue());
                writer.flush();
                writer.newLine();
            }
            System.out.println("============================成功处理【" + totalCount + "】条数据============================");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String getLineitemKey(String line) {
        String[] splits = line.split("\\|");
        if (splits.length > 0) {
            return splits[2];
        }
        return null;
    }
}
