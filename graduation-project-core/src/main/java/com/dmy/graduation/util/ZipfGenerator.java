package com.dmy.graduation.util;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by DMY on 2018/10/25 10:36
 */
public class ZipfGenerator implements Serializable {

    private Random random = new Random(0);

    private NavigableMap<Double, Integer> map;

    private static final double Constant = 1.0;

    private static final long GB = 1024 * 1024 * 1024;

    private static final int SIZE = 100000;

    private static AtomicInteger atomicInteger = new AtomicInteger(1);

    private static ThreadPoolExecutor threadPool =
            new ThreadPoolExecutor(5, 6, 5, TimeUnit.SECONDS, new LinkedBlockingDeque<>());

    private int size;

    private double skew;

    public ZipfGenerator(int size, double skew) {
        // create the TreeMap
        this.size = size;
        this.skew = skew;
        map = computeMap();
    }

    //size为rank个数，skew为数据倾斜程度
    private NavigableMap<Double, Integer> computeMap() {
        NavigableMap<Double, Integer> map = new TreeMap<>();
        //总频率
        double div = 0;
        //对每个rank，计算对应的词频，计算总词频
        for (int i = 1; i <= size; i++) {
            //the frequency in position i
            div += (Constant / Math.pow(i, skew));
        }
        //计算每个rank对应的y值，所以靠前rank的y值区间远比后面rank的y值区间大
        double sum = 0;
        for (int i = 1; i <= size; i++) {
            double p = (Constant / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum, i - 1);
        }
        return map;
    }

    private int next() {         // [1,n]
        double value = random.nextDouble();
        //找最近y值对应的rank
        return map.ceilingEntry(value).getValue() + 1;
    }

    public void generateData(String filePrefix, String filePath) {
        Map<String, Integer> keyCountMap = new HashMap<>(SIZE);
        for (int i = 1; i <= 10; i++) {
            Thread thread = new Thread(() -> {
                System.out.println("===================" + Thread.currentThread().getName() + "正在生成数据===================");
                String fileName = filePrefix + "_" + atomicInteger.getAndIncrement() + ".txt";
                File file = new File(filePath + fileName);
                String dataPrefix = "spark_partitioner_key_";
                BufferedWriter writer1 = null;
                BufferedWriter writer2 = null;
                try {
                    writer1 = new BufferedWriter(new FileWriter(filePath + fileName));
                    int count = 1;
                    while (file.length() < GB) {
                        String key = dataPrefix + next();
                        keyCountMap.put(key, keyCountMap.getOrDefault(key, 0) + 1);
                        if (count < 100) {
                            writer1.write(key + "|");
                        } else {
                            writer1.write(key);
                        }
                        if (count == 100) {
                            count = 0;
                            writer1.newLine();
                            writer1.flush();
                        }
                        count++;
                    }
                    writer1.flush();
                    fileName = filePrefix + "_" + "KeyCount";
                    writer2 = new BufferedWriter(new FileWriter(filePath + fileName));
                    for (Map.Entry<String, Integer> entry : keyCountMap.entrySet()) {
                        writer2.write(entry.getKey() + ":" + entry.getValue());
                        writer2.newLine();
                        writer2.flush();
                    }
                    System.out.println("===================" + Thread.currentThread().getName() + "成功生成数据===================");
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (writer1 != null) {
                            writer1.close();
                        }
                        if (writer2 != null) {
                            writer2.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            threadPool.execute(thread);
        }
        threadPool.shutdown();
    }

    public static void main(String[] args) {
        ZipfGenerator zipfGenerator = new ZipfGenerator(SIZE, 0.0);
        zipfGenerator.generateData("10_0.0", "F:\\研究内容相关资料\\毕设相关\\experiment_data\\");
    }
}
