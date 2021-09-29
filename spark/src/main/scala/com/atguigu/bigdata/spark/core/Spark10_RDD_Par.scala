package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Par {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // 5byte
    // 5 / 2 = 2

    // 2 + 2 + 1 => 3

    // 0 => (0, 2)  => 12345
    // 1 => (2, 4)
    // 2 => (4, 5)

    val rdd = sc.textFile("input/word.txt", 2)
    rdd.saveAsTextFile("output")

    sc.stop

  }

}
