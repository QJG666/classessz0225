package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Par {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    // conf.set("spark.default.parallelism", "4")

    val sc = new SparkContext(conf)

    // 数据分区的源码
    /*
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
      ....
      0 => (0, 1)  => 1
      1 => (1, 3)  => 2, 3
      2 => (3, 5)  => 4, 5
      array.slice(start, end).toSeq
     */
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), 3)

    rdd.saveAsTextFile("output")



    sc.stop

  }

}
