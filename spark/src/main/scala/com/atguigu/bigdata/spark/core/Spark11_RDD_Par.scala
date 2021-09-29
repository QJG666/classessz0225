package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Par {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // 3 + 3 => 6byte
    // 6 / 3 = 2
    // 2 + 2 + 2 => 3
    // 3 / 2 + 3 / 2 =
    // 1 + 1 + 1 + 1 => 4
    // hadoop计算分区数量是按照所有文件的总的字节数
    // 但是真正进行分区的时候，是按照文件来分区

    val rdd = sc.textFile("input", 3)
    rdd.saveAsTextFile("output")

    sc.stop

  }

}
