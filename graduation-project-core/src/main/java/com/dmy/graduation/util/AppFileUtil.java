package com.dmy.graduation.util;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by DMY on 2018/10/23 15:05
 */
public class AppFileUtil extends FileUtil {

    /**
     * key: app标识符   value: app名称
     */
    private Map<String, String> appSymbolMap = new HashMap<>(INITIAL_CAPACITY);

    /**
     * key: app名称     value: app被使用次数
     */
    private Map<String, Integer> generatedAppVisitCountMap = new HashMap<>(INITIAL_CAPACITY);

    private ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(4, 6,
            5, TimeUnit.SECONDS, new LinkedBlockingDeque<>(), new NamedThreadFactory("Load-File-Pool"));

    /**
     * 记录数据条数
     */
    private AtomicLong totalDataCount = new AtomicLong(0L);

    /**
     * 产生均匀的App访问数据量
     */
    public void generateAppVisitCount() {
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                    AppFileUtil.class.getClassLoader().getResourceAsStream("files/AppVisitCount3.txt")));
            String line = null;
            Random random = new Random();

            while ((line = bufferedReader.readLine()) != null) {
                String[] splits = line.split(":");
                generatedAppVisitCountMap.put(splits[0], random.nextInt(2000) + 10000);
            }

            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(RESOURCE_FILE_PATH + "\\AppVisitCount3.txt"));
            for (Map.Entry<String, Integer> entry : generatedAppVisitCountMap.entrySet()) {
                try {
                    bufferedWriter.write(entry.getKey() + ":" + entry.getValue());
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从excel文件中获取App名称与其标签映射关系
     */
    private void initAppSymbol() {
        try {
            Workbook workbook = WorkbookFactory.create(AppFileUtil.class.getClassLoader().getResourceAsStream("files/AppSymbol.xls"));
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

    public void getAppVisitCount(String foldPath) {
        //initAppSymbol();
        initKeyCount("AppVisitCount4.txt");
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
                                //String appSymbol = splits[16];
                                //String appName = appSymbolMap.get(appSymbol);
                                String user = splits[1];
                                hashMap.put(user, hashMap.getOrDefault(user, 0) + 1);
                            }
                            synchronized (keyCountMap) {
                                hashMap.forEach((appName, visitCount) ->
                                        keyCountMap.put(appName, keyCountMap.getOrDefault(appName, 0) + visitCount));
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
                        new FileWriter(RESOURCE_FILE_PATH + "\\userVisitLogCount.txt"));
                long dealedDataCount = 0L;
                for (Map.Entry<String, Integer> entry : keyCountMap.entrySet()) {
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

}
