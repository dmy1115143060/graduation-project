package com.dmy.graduation.util;


import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by DMY on 2018/10/9 16:12
 */
public class FileUtil {

    /**
     * 从excel文件中获取APP名称与其标签映射关系
     */
    public static Map<String, String> getAppMap() {
        Map<String, String> appSymbolMap = new HashMap<>(1000);
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
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidFormatException e) {
            e.printStackTrace();
        }
        return appSymbolMap;
    }

    public static void main(String[] args) {
        System.out.println(getAppMap());
    }
}
