package com.atguigu.bigdata.spark.test;

import java.util.Random;

public class TestRandom {
    public static void main(String[] args) {

        // 随机数不随机
        // 所谓的随机数其实是通过随机算法计算出来的
        // 10 => 01010101 => 1111 => 01111010 => 1100 => ...
        // 10 => 01010101 => 1111 => 01111010 => 1100 => ...
        Random r1 = new Random(10);
        for ( int i = 1; i <= 5; i++ ) {
            System.out.println(r1.nextInt(10));
        }

        System.out.println("********************************");

        Random r2 = new Random(10);
        for ( int i = 1; i <= 5; i++ ) {
            System.out.println(r2.nextInt(10));
        }
    }
}
