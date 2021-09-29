package com.atguigu.bigdata.spark.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Action3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("rdd").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO Spark - RDD - 行动算子
    val rdd = sc.makeRDD(List("Hello", "world", "Hello", "Scala"))

    // WordCount
    // val stringToLong = rdd.map((_, 1)).countByKey()

    // WordCount
    val stringToLong = rdd.countByValue()

    println(stringToLong)


    sc.stop()
  }

}
