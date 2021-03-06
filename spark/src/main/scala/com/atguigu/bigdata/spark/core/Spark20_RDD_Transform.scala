package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Transform {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    val list = List(
      List(1, 2), List(3, 4), List(5, 6)
    )
    val rdd = sc.makeRDD(list, 2)

    val rdd1 = rdd.flatMap(
      list => {
        list
      }
    )
    println(rdd1.collect().mkString(","))


    sc.stop

  }

}
