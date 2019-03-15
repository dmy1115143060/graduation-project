package com.dmy.graduation.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by DMY on 2018/11/12 15:26
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(locations = "classpath*:/applicationTest.xml")
public class BaseTest {

    @Test
    public void test() {
        Scanner input = new Scanner(System.in);
        String line = input.nextLine();
        String[] splits = line.split("\\s+");
        int totalCount = Integer.parseInt(splits[0]);
        int totalMoney = Integer.parseInt(splits[1]);
        int[] weight = new int[totalCount];
        double[] value = new double[totalCount];
        for (int i = 0; i < totalCount; i++) {
            line = input.nextLine();
            splits = line.split("\\s+");
            weight[i] = Integer.parseInt(splits[0]);
            value[i] = Double.parseDouble(splits[1]);
        }
        ZeroOnePackage(value, weight, totalMoney, new ArrayList<Integer>());
    }

    public void ZeroOnePackage(double[] value, int[] weight, int capacity, List<Integer> idList) {
        double[][] M = new double[value.length][capacity + 1];
        double max = 0d;
        for (int i = 0; i < value.length; i++) {
            if (weight[i] <= capacity)
                for (int j = weight[i]; j <= capacity; j++)
                    M[i][j] = value[i];
        }
        for (int i = 0; i < value.length; i++) {
            for (int j = 0; j <= capacity; j++) {
                if (i > 0 && j - weight[i] >= 0) {
                    double v1 = M[i - 1][j];
                    double v2 = M[i - 1][j - weight[i]] + value[i];
                    if (Double.compare(v1, v2) > 0) {
                        M[i][j] = v1;
                    } else {
                        M[i][j] = v2;
                        idList.add(i);
                    }
                }
                if (Double.compare(M[i][j], max) > 0) {
                    max = M[i][j];
                }
            }
        }
        System.out.println(max);
        for (int id : idList) {
            System.out.print(id + "\t");
        }
        System.out.println();
    }
}
