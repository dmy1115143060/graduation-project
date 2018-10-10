package com.dmy.graduation.util;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by DMY on 2018/10/9 16:12
 */
public class FileUtil {

    private static final String RESOURCE_FILE_PATH = "G:\\Intellij\\graduation-project\\graduation-project-core\\src\\main\\resources\\files";
    private static final int INITIAL_CAPACITY = 1000;

    /**
     * key: app标识符   value: app名称
     */
    private static final Map<String, String> appSymbolMap = new HashMap<>(INITIAL_CAPACITY);

    /**
     * key: app名称     value: app被使用次数
     */
    private static final Map<String, Integer> appVisitCountMap = new HashMap<>(INITIAL_CAPACITY);

    private static AtomicLong totalDataCount = new AtomicLong(0L);

    /**
     * 线程池
     */
    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(4, 6,
            5, TimeUnit.SECONDS, new LinkedBlockingDeque<>(), new NamedThreadFactory("Load-File-Pool"));

    private static void initAppVisitCount() {
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                    FileUtil.class.getClassLoader().getResourceAsStream("files/AppVisitCount.txt")));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] splits = line.split(":");
                appVisitCountMap.put(splits[0], Integer.parseInt(splits[1]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从excel文件中获取App名称与其标签映射关系
     */
    public static void initAppSymbol() {
        try {
            Workbook workbook = WorkbookFactory.create(FileUtil.class.getClassLoader().getResourceAsStream("files/AppSymbol.xls"));
            Sheet sheet = workbook.getSheetAt(0);
            DataFormatter formatter = new DataFormatter();
            for (Row row : sheet) {
                Cell cell1 = row.getCell(0);
                Cell cell2 = row.getCell(1);
                String key = formatter.formatCellValue(cell1);
                String value = formatter.formatCellValue(cell2);
                appSymbolMap.put(key, value);
            }
        } catch (InvalidFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getAppVisitCount(String foldPath) {
        initAppSymbol();
        initAppVisitCount();
        System.out.println("appSymbolMap: " + appSymbolMap.size());
        System.out.println("appVisitCountMap: " + appVisitCountMap.size());
        try {
            // 加载该目录下所有的文件
            File fold = new File(foldPath);
            if (fold.isDirectory()) {
                File[] fileList = fold.listFiles();
                assert (fileList != null && fileList.length > 0);
                CountDownLatch countDownLatch = new CountDownLatch(fileList.length);

                // 对于每一个文件，将其封装成一个任务利用线程池处理
                for (File file : fileList) {
                    Thread t = new Thread(() -> {
                        System.out.println(Thread.currentThread().getName() + "正在处理文件: " + file.getName());
                        BufferedReader bufferedReader = null;
                        try {
                            bufferedReader = new BufferedReader(new FileReader(file));
                            String line = null;
                            HashMap<String, Integer> hashMap = new HashMap<>(INITIAL_CAPACITY);
                            while ((line = bufferedReader.readLine()) != null) {
                                totalDataCount.incrementAndGet();
                                String[] splits = line.split("\\|");
                                String appSymbol = splits[16];
                                String appName = appSymbolMap.get(appSymbol);
                                hashMap.put(appName, hashMap.getOrDefault(appName, 0) + 1);
                            }
                            synchronized (appVisitCountMap) {
                                hashMap.forEach((appName, visitCount) ->
                                    appVisitCountMap.put(appName, appVisitCountMap.getOrDefault(appName, 0) + visitCount));
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            countDownLatch.countDown();
                            System.out.println(Thread.currentThread().getName() + "成功处理文件: " + file.getName());
                            try {
                                bufferedReader.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                    threadPoolExecutor.execute(t);
                }

                // 等待所有子线程完成
                countDownLatch.await();
                System.out.println("子线程处理完成！");
                threadPoolExecutor.shutdown();

                // 处理结果写入文件当中
                BufferedWriter bufferedWriter = new BufferedWriter(
                        new FileWriter(RESOURCE_FILE_PATH + "//AppVisitCount.txt"));
                long dealedDataCount = 0L;
                for (Map.Entry<String, Integer> entry : appVisitCountMap.entrySet()) {
                    dealedDataCount += entry.getValue();
                    bufferedWriter.write(entry.getKey() + ":" + entry.getValue());
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                }
                System.out.println("totalCount: " + totalDataCount.get());
                System.out.println("dataCount: " + dealedDataCount);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        getAppVisitCount("F:\\学习资料\\我的资源\\电信数据\\0418");
    }
}
