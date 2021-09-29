package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // TODO : 将List(List(1,2),3, 4, List(5,6))进行扁平化操作

    val rdd = sc.makeRDD(List(
      List(1, 2),
      3,
      4,
      List(5, 6)
    ), 1)

    val rdd1 = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case d => List(d)
        }
      }
    )
    rdd1.collect().foreach(println)


    sc.stop

  }

}
